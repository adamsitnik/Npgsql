using Npgsql.FrontendMessages;
using Npgsql.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Npgsql
{
    // TODO: Implement IAsyncDisposable
    /// <summary>
    /// Represents a set or batch of commands, which can be executed in a single database roundtrip for better performance.
    /// </summary>
    public class NpgsqlCommandSet : IDisposable
    {
        #region Fields and Properties

        public List<NpgsqlCommand> Commands { get; private set; } = new List<NpgsqlCommand>();

        internal bool FlushOccurred { get; set; }

        bool _isDisposed;

        static readonly SingleThreadSynchronizationContext SingleThreadSynchronizationContext = new SingleThreadSynchronizationContext("NpgsqlRemainingAsyncSendWorker");

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        #endregion Fields and Properties

        #region Public properties

        public int Timeout { get; set; }

        public NpgsqlConnection Connection { get; set; }
        protected DbConnection DbConnection
        {
            get => Connection;
            set => Connection = (NpgsqlConnection)value;
        }

        public NpgsqlTransaction Transaction { get; set; }
        protected DbTransaction DbTransaction {
            get => Transaction;
            set => Transaction = (NpgsqlTransaction)value;
        }

        #endregion Public properties

        #region Execution (mirrors DbCommand)

        public DbDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteReader(behavior, false, CancellationToken.None).GetAwaiter().GetResult();

        public Task<DbDataReader> ExecuteReaderAsync(CommandBehavior behavior = CommandBehavior.Default, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using (NoSynchronizationContextScope.Enter())
                return ExecuteReader(behavior, true, cancellationToken).AsTask();
        }

        internal async ValueTask<DbDataReader> ExecuteReader(CommandBehavior behavior, bool async, CancellationToken cancellationToken, bool preprocessed = false)
        {
            var connector = CheckReadyAndGetConnector();

            var rawMode = Connection.Settings.RawSqlMode;
            foreach (var cmd in Commands)
            {
                cmd.Preprocess(connector);
                if (!rawMode && cmd.CommandSet.Commands.Count > 1)
                    throw new NotSupportedException("Multiple statements separated by semicolons found in CommandText, not supported when using DbCommandSet.");
            }

            connector.StartUserAction(this);
            try
            {
                using (cancellationToken.Register(cmdSet => ((NpgsqlCommandSet)cmdSet).Cancel(), this))
                {
                    // TODO: Logging
                    //if (Log.IsEnabled(NpgsqlLogLevel.Debug))
                    //    LogCommand();
                    Task sendTask;

                    // If a cancellation is in progress, wait for it to "complete" before proceeding (#615)
                    lock (connector.CancelLock) { }

                    connector.UserTimeout = Timeout * 1000;

                    if ((behavior & CommandBehavior.SchemaOnly) == 0)
                    {
                        /*
                        if (connector.Settings.MaxAutoPrepare > 0)
                        {
                            foreach (var statement in _statements)
                            {
                                // If this statement isn't prepared, see if it gets implicitly prepared.
                                // Note that this may return null (not enough usages for automatic preparation).
                                if (!statement.IsPrepared)
                                    statement.PreparedStatement =
                                        connector.PreparedStatementManager.TryGetAutoPrepared(statement);
                                if (statement.PreparedStatement != null)
                                    statement.PreparedStatement.LastUsed = DateTime.UtcNow;
                            }
                            _connectorPreparedOn = connector;
                        }*/

                        // We do not wait for the entire send to complete before proceeding to reading -
                        // the sending continues in parallel with the user's reading. Waiting for the
                        // entire send to complete would trigger a deadlock for multi-statement commands,
                        // where PostgreSQL sends large results for the first statement, while we're sending large
                        // parameter data for the second. See #641.
                        // Instead, all sends for non-first statements and for non-first buffers are performed
                        // asynchronously (even if the user requested sync), in a special synchronization context
                        // to prevents a dependency on the thread pool (which would also trigger deadlocks).
                        // The WriteBuffer notifies this command when the first buffer flush occurs, so that the
                        // send functions can switch to the special async mode when needed.
                        sendTask = SendExecute(async);
                    }
                    else
                    {
                        throw new NotImplementedException();
                        //sendTask = SendExecuteSchemaOnly(async);
                    }

                    // The following is a hack. It raises an exception if one was thrown in the first phases
                    // of the send (i.e. in parts of the send that executed synchronously). Exceptions may
                    // still happen later and aren't properly handled. See #1323.
                    if (sendTask.IsFaulted)
                        sendTask.GetAwaiter().GetResult();

                    //var reader = new NpgsqlDataReader(this, behavior, _statements, sendTask);
                    var reader = connector.DataReader;
                    reader.Init(Connection, Commands, behavior, sendTask);
                    connector.CurrentReader = reader;
                    if (async)
                        await reader.NextResultAsync(cancellationToken);
                    else
                        reader.NextResult();
                    return reader;
                }
            }
            catch
            {
                Connection.Connector?.EndUserAction();

                // Close connection if requested even when there is an error.
                if ((behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
                    Connection.Close();
                throw;
            }
        }

        async Task SendExecute(bool async)
        {
            BeginSend();
            var connector = Connection.Connector;
            Debug.Assert(connector != null);

            var buf = connector.WriteBuffer;
            for (var i = 0; i < Commands.Count; i++)
            {
                async = ForceAsyncIfNecessary(async, i);
                await Commands[i].WriteExecuteSingle(connector, async);
            }
            await SyncMessage.Instance.Write(buf, async);
            await buf.Flush(async);
            CleanupSend();
        }

        /*
        async Task SendExecuteSchemaOnly(bool async)
        {
            var connector = Connection.Connector;
            Debug.Assert(connector != null);

            var wroteSomething = false;

            var buf = connector.WriteBuffer;
            for (var i = 0; i < Commands.Count; i++)
            {
                var command = Commands[i];

                if (command.PreparedStatement?.State == PreparedState.Prepared)
                    continue;   // Prepared, we already have the RowDescription
                Debug.Assert(command.PreparedStatement == null);

                await connector.ParseMessage
                    .Populate(command.RawCommandText, "", command.Parameters, connector.TypeMapper)
                    .Write(buf, async);

                await connector.DescribeMessage
                    .Populate(StatementOrPortal.Statement, command.StatementName)
                    .Write(buf, async);
                wroteSomething = true;
            }

            if (wroteSomething)
            {
                await SyncMessage.Instance.Write(buf, async);
                await buf.Flush(async);
            }
        }

        async Task SendDeriveParameters(bool async)
        {
            var connector = Connection.Connector;
            Debug.Assert(connector != null);
            var buf = connector.WriteBuffer;
            for (var i = 0; i < Commands.Count; i++)
            {
                await connector.ParseMessage
                    .Populate(Commands[i].RawCommandText, string.Empty)
                    .Write(buf, async);

                await connector.DescribeMessage
                    .Populate(StatementOrPortal.Statement, string.Empty)
                    .Write(buf, async);
            }
            await SyncMessage.Instance.Write(buf, async);
            await buf.Flush(async);
        }

        async Task SendPrepare(bool async)
        {
            var connector = Connection.Connector;
            Debug.Assert(connector != null);
            var buf = connector.WriteBuffer;
            for (var i = 0; i < Commands.Count; i++)
            {
                var command = Commands[i];
                var pStatement = command.PreparedStatement;

                // A statement may be already prepared, already in preparation (i.e. same statement twice
                // in the same command), or we can't prepare (overloaded SQL)
                if (pStatement?.State != PreparedState.ToBePrepared)
                    continue;

                var statementToClose = pStatement?.StatementBeingReplaced;
                if (statementToClose != null)
                {
                    // We have a prepared statement that replaces an existing statement - close the latter first.
                    await connector.CloseMessage
                        .Populate(StatementOrPortal.Statement, statementToClose.Name)
                        .Write(buf, async);
                }

                await connector.ParseMessage
                    .Populate(command.RawCommandText, pStatement.Name, command.Parameters, connector.TypeMapper)
                    .Write(buf, async);

                await connector.DescribeMessage
                    .Populate(StatementOrPortal.Statement, pStatement.Name)
                    .Write(buf, async);

                pStatement.State = PreparedState.BeingPrepared;
            }
            await SyncMessage.Instance.Write(buf, async);
            await buf.Flush(async);
        }

        async Task SendClose(bool async)
        {
            var connector = Connection.Connector;
            Debug.Assert(connector != null);

            var buf = connector.WriteBuffer;
            foreach (var command in Commands.Where(s => s.IsPrepared))
            {
                await connector.CloseMessage
                    .Populate(StatementOrPortal.Statement, command.StatementName)
                    .Write(buf, async);
                Debug.Assert(command.PreparedStatement != null);
                command.PreparedStatement.State = PreparedState.BeingUnprepared;
            }
            await SyncMessage.Instance.Write(buf, async);
            await buf.Flush(async);
        }
        */
        void BeginSend()
        {
            Debug.Assert(Connection?.Connector != null);
            Connection.Connector.WriteBuffer.CurrentCommandSet = this;
            FlushOccurred = false;
        }

        void CleanupSend()
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (SynchronizationContext.Current != null)  // Check first because SetSynchronizationContext allocates
                SynchronizationContext.SetSynchronizationContext(null);
        }

        bool ForceAsyncIfNecessary(bool async, int numberOfStatementInBatch)
        {
            if (!async && FlushOccurred && numberOfStatementInBatch > 0)
            {
                // We're synchronously sending the non-first statement in a batch and a flush
                // has already occured. Switch to async. See long comment in Execute() above.
                async = true;
                SynchronizationContext.SetSynchronizationContext(SingleThreadSynchronizationContext);
            }
            return async;
        }

        #endregion Execution
    
        #region Other methods mirroring DbCommand
    
        public void Prepare()
        {
            throw new NotImplementedException();
        }

        public Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns whether this query will execute as a prepared (compiled) query.
        /// </summary>
        public bool IsPrepared => throw new NotImplementedException();

        public void Cancel()
        {
            throw new NotImplementedException();
        }

        public Task CancelAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
        
        #endregion

        /// <inheritdoc />
        public void Dispose() => _isDisposed = true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        NpgsqlConnector CheckReadyAndGetConnector()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(GetType().FullName);
            if (Connection == null)
                throw new InvalidOperationException("Connection property has not been initialized.");
            return Connection.CheckReadyAndGetConnector();
        }
    }
}
