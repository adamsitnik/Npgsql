using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using NpgsqlTypes;

namespace Npgsql
{
    class CachedCommandManager
    {
        internal int MaxAutoPrepared { get; }
        internal int UsagesBeforePrepare { get; }

        internal Dictionary<string, CachedCommand> Commands { get; } = new Dictionary<string, CachedCommand>();
        internal Dictionary<string, NpgsqlStatement> PreparedStatements { get; } = new Dictionary<string, NpgsqlStatement>();

        [ItemCanBeNull]
        readonly CachedCommand[] _autoPreparedCommands;
        [ItemCanBeNull]
        readonly NpgsqlStatement[] _autoPreparedStatements;

        [CanBeNull, ItemCanBeNull]
        readonly CachedCommand[] _candidates;

        /// <summary>
        /// Total number of current prepared statements (whether explicit or automatic).
        /// </summary>
        internal int NumPrepared;
        // TODO: Update this...!

        readonly NpgsqlConnector _connector;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        internal const int CandidateCount = 100;

        internal CachedCommandManager(NpgsqlConnector connector)
        {
            _connector = connector;
            MaxAutoPrepared = connector.Settings.MaxAutoPrepare;
            UsagesBeforePrepare = connector.Settings.AutoPrepareMinUsages;
            if (MaxAutoPrepared > 0)
            {
                if (MaxAutoPrepared > 256)
                    Log.Warn($"{nameof(MaxAutoPrepared)} is over 256, performance degradation may occur. Please report via an issue.", connector.Id);
                _autoPreparedCommands = new CachedCommand[MaxAutoPrepared];
                _autoPreparedStatements = new NpgsqlStatement[MaxAutoPrepared];
                _candidates = new CachedCommand[CandidateCount];
            }
        }

        internal void TryAutoPrepare(NpgsqlCommand command)
        {
            Debug.Assert(_candidates != null);

            var sql = command.CommandText;

            var cachedCommand = command._cachedCommand;
            Debug.Assert(cachedCommand?.IsPrepared != true, $"Attached to prepared command but gone into {nameof(TryAutoPrepare)}");

            if (cachedCommand == null)
            {
                // We're not attached to a cached command. Try to find one in the command cache.
                if (Commands.TryGetValue(sql, out cachedCommand))
                {
                    if (cachedCommand.IsPrepared)
                    {
                        // We've found a cached prepared command.
                        // We just need to check that the parameter types correspond, since prepared statements are
                        // only keyed by SQL (to prevent pointless allocations). If we have a mismatch, simply run unprepared.
                        // TODO: Do we really want to spend this time
                        //if (cachedCommand.DoParametersMatch(command.Parameters))
                        {
                            command._cachedCommand = cachedCommand;
                            command._statements = cachedCommand.Statements;
                            cachedCommand.RefCount++;
                        }
                        //else
                        //    command.ProcessRawQuery();
                        return;
                    }
                }
                else
                {
                    // The command's SQL doesn't exist in our cache. Parse it and create a new candidate in our
                    // cache, either in an empty candidate slot of ejecting a least-used one.
                    command.ProcessRawQuery();

                    int slotIndex = -1, leastUsages = int.MaxValue;
                    var lastUsed = DateTime.MaxValue;
                    for (var i = 0; i < _candidates.Length; i++)
                    {
                        var candidate = _candidates[i];
                        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                        // ReSharper disable HeuristicUnreachableCode
                        if (candidate == null) // Found an unused candidate slot, return immediately
                        {
                            slotIndex = i;
                            break;
                        }
                        // ReSharper restore HeuristicUnreachableCode

                        if (candidate.Usages < leastUsages)
                        {
                            leastUsages = candidate.Usages;
                            slotIndex = i;
                            lastUsed = candidate.LastUsed;
                        }
                        else if (candidate.Usages == leastUsages && candidate.LastUsed < lastUsed)
                        {
                            slotIndex = i;
                            lastUsed = candidate.LastUsed;
                        }
                    }

                    var leastUsed = _candidates[slotIndex];
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (leastUsed != null)
                        Commands.Remove(leastUsed.Sql);
                    cachedCommand = Commands[sql] =
                        _candidates[slotIndex] = new CachedCommand(sql, command._statements);
                }

                // Attach our command to the cached command
                command._cachedCommand = cachedCommand;
                command._statements = cachedCommand.Statements;
                cachedCommand.RefCount++;
            }

            if (++cachedCommand.Usages < UsagesBeforePrepare)
            {
                // Command still hasn't passed the usage threshold, no automatic preparation.
                // Execute unprepared.
                cachedCommand.LastUsed = DateTime.UtcNow;
                return;
            }

            // Bingo, we've just passed the usage threshold, command should get prepared
            Log.Trace($"Automatically preparing command: {sql}", _connector.Id);

            SetupPreparedStatements(cachedCommand, false);

            // Add our new command
            int commandSlot;
            for (commandSlot = 0; commandSlot < _autoPreparedCommands.Length; commandSlot++)
                if (_autoPreparedCommands[commandSlot] == null)
                    break;
            if (commandSlot == _autoPreparedCommands.Length)
            {
                // This only happens if all the autoprepared commands are referenced by non-disposed commands...
                // Leave this command as a candidate
                return;
            }

            _autoPreparedCommands[commandSlot] = cachedCommand;

            RemoveCandidate(cachedCommand);

            // Note that the parameter types are only set at the moment of preparation - in the candidate phase
            // there's no differentiation between overloaded statements, which are a pretty rare case, saving
            // allocations.
            cachedCommand.SetParamTypes(command.Parameters);

            cachedCommand.IsPrepared = cachedCommand.IsAutoPrepared = true;
        }

        internal void SetupPreparedStatements(CachedCommand cachedCommand, bool isExplicit)
        {
            var preparedStatementsAllocated = 0;

            for (var statementIndex = 0; statementIndex < cachedCommand.Statements.Count; statementIndex++)
            {
                var statement = cachedCommand.Statements[statementIndex];
                if (statement.IsPrepared)
                {
                    // Our command is referencing an already-prepared statement is already prepared
                    // (i.e. referenced by some other command). Nothing to do.
                    continue;
                }

                var sql = statement.SQL;
                if (PreparedStatements.TryGetValue(sql, out var pStatement))
                {
                    Debug.Assert(pStatement.IsPrepared,
                        $"While preparing, found statement in {nameof(CachedCommandManager)} with state {pStatement.State}");
                    cachedCommand.Statements[statementIndex] = pStatement;
                    pStatement.RefCount++;
                    // TODO: Check types on a per-statement level?
                    continue;
                }

                // Statement hasn't been prepared yet

                // The current command may contain more statements than MaxAutoPrepared (theoretically...)
                if (preparedStatementsAllocated == MaxAutoPrepared)
                    continue;

                PreparedStatements[sql] = statement;

                //statement.PreparedStatement.SetParamTypes(statement.InputParameters);

                // First, see if a free autoprepare statement slot exists
                int statementSlot;
                for (statementSlot = 0; statementSlot < _autoPreparedStatements.Length; statementSlot++)
                {
                    var ps = _autoPreparedStatements[statementSlot];
                    if (ps == null || ps.RefCount == 0)
                        break;
                }

                if (statementSlot == _autoPreparedStatements.Length)
                {
                    // All autoprepare statement slots are in use.
                    // Go over all autoprepared commands, find the LRU to release.
                    var oldestTimestamp = DateTime.MaxValue;
                    var oldestIndex = -1;
                    CachedCommand oldestCommand = null;
                    for (var i = 0; i < _autoPreparedCommands.Length; i++)
                    {
                        var pc = _autoPreparedCommands[i];
                        if (pc?.RefCount == 0 && pc.LastUsed < oldestTimestamp)
                        {
                            oldestCommand = pc;
                            oldestIndex = i;
                            oldestTimestamp = pc.LastUsed;
                        }
                    }

                    if (oldestCommand == null)
                    {
                        // All the autoprepared commands are referenced by non-disposed commands... Can't autoprepare.
                        continue;
                    }

                    // We have our LRU. Remove it from all our bookkeeping structures.
                    Debug.Assert(oldestCommand.RefCount == 0);
                    Debug.Assert(oldestCommand.IsAutoPrepared);
                    statement.CommandBeingReplaced = oldestCommand;
                    _autoPreparedCommands[oldestIndex] = null;
                    Commands.Remove(oldestCommand.Sql);

                    // Detach all the LRU's prepared statements
                    foreach (var replacedStatement in oldestCommand.Statements)
                    {
                        Debug.Assert(replacedStatement.IsPrepared);
                        if (--replacedStatement.RefCount == 0)
                            PreparedStatements.Remove(replacedStatement.SQL);
                    }

                    // Now scan the prepared statement list again to find the first empty slot
                    for (statementSlot = 0; statementSlot < _autoPreparedStatements.Length; statementSlot++)
                        if (_autoPreparedStatements[statementSlot].RefCount == 0)
                            break;

                    Debug.Assert(statementSlot < _autoPreparedStatements.Length,
                        "No free prepared statement slot found after removing LRU autoprepare command!");
                }

                preparedStatementsAllocated++;
                statement.Name = "_auto" + statementSlot;
                _autoPreparedStatements[statementSlot] = pStatement;
            }
        }



        void RemoveCandidate(CachedCommand candidate)
        {
            Debug.Assert(_candidates != null);
            var i = 0;
            for (; i < _candidates.Length; i++)
            {
                if (_candidates[i] == candidate)
                {
                    _candidates[i] = null;
                    return;
                }
            }
            Debug.Assert(i < _candidates.Length);
        }

        internal void ClearAll()
        {
            Commands.Clear();
            NumPrepared = 0;
            if (_candidates != null)
                for (var i = 0; i < _candidates.Length; i++)
                    _candidates[i] = null;
        }
    }
}
