﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Logging;
using System.Transactions;

namespace Npgsql
{
    /// <summary>
    /// Provides lookup for a pool based on a connection string.
    /// </summary>
    /// <remarks>
    /// <see cref="TryGetValue"/> is lock-free, to avoid contention, but the same isn't
    /// true of <see cref="GetOrAdd"/>, which acquires a lock. The calling code always tries
    /// <see cref="TryGetValue"/> before trying to <see cref="GetOrAdd"/>.
    /// </remarks>
    static class PoolManager
    {
        internal const int InitialPoolsSize = 10;

        static (string Key, ConnectorPool Pool)[] _pools = new (string, ConnectorPool)[InitialPoolsSize];
        static int _nextSlot;

        internal static bool TryGetValue(string key, out ConnectorPool pool)
        {
            // Note that pools never get removed. _pools is strictly append-only.
            var pools = _pools;

            // First scan the pools and do reference equality on the connection strings
            for (var i = 0; i < _nextSlot; i++)
            {
                if (ReferenceEquals(pools[i].Key, key))
                {
                    // It's possible that this pool entry is currently being written: the connection string
                    // component has already been writte, but the pool component is just about to be. So we
                    // loop on the pool until it's non-null
                    while (pools[i].Pool == null)
                        Thread.Sleep(10);
                    pool = pools[i].Pool;
                    return true;
                }
            }

            // Next try value comparison on the strings
            for (var i = 0; i < _nextSlot; i++)
            {
                if (pools[i].Key == key)
                {
                    // See comment above
                    while (pools[i].Pool == null)
                        Thread.Sleep(10);
                    pool = pools[i].Pool;
                    return true;
                }
            }

            pool = null;
            return false;
        }

        internal static ConnectorPool GetOrAdd(string key, ConnectorPool pool)
        {
            lock (_pools)
            {
                if (TryGetValue(key, out var result))
                    return result;

                // May need to grow the array.
                if (_nextSlot == _pools.Length)
                {
                    var newPools = new (string, ConnectorPool)[_pools.Length * 2];
                    Array.Copy(_pools, newPools, _pools.Length);
                    Interlocked.Exchange(ref _pools, newPools);
                }

                _pools[_nextSlot].Key = key;
                _pools[_nextSlot].Pool = pool;
                Interlocked.Increment(ref _nextSlot);
                return pool;
            }
        }

        internal static void Clear(string connString)
        {
            Debug.Assert(connString != null);

            if (TryGetValue(connString, out var pool))
                pool.Clear();
        }

        internal static void ClearAll()
        {
            for (var i = 0; i < _nextSlot; i++)
            {
                if (_pools[i].Key == null)
                    return;
                _pools[i].Pool?.Clear();
            }
        }

        static PoolManager()
        {
            // When the appdomain gets unloaded (e.g. web app redeployment) attempt to nicely
            // close idle connectors to prevent errors in PostgreSQL logs (#491).
            AppDomain.CurrentDomain.DomainUnload += (sender, args) => ClearAll();
            AppDomain.CurrentDomain.ProcessExit += (sender, args) => ClearAll();
        }

#if DEBUG
        /// <summary>
        /// Resets the pool manager to its initial state, for test purposes only.
        /// Assumes that no other threads are accessing the pool.
        /// </summary>
        internal static void Reset()
        {
            lock (_pools)
            {
                ClearAll();
                _pools = new (string, ConnectorPool)[InitialPoolsSize];
                _nextSlot = 0;
            }
        }
#endif
    }

    /// <summary>
    /// Connection pool for PostgreSQL physical connections. Implementation is completely lock-free
    /// to avoid contention.
    /// </summary>
    /// <remarks>
    /// When the number of physical connections reaches MaxPoolSize, further attempts to allocate
    /// connections will block until an existing connection is released. If multiple waiters
    /// exist, they will receive connections in FIFO manner to ensure fairness and prevent old waiters
    /// to time out.
    /// </remarks>
    sealed class ConnectorPool : IDisposable
    {
        #region Fields

        internal NpgsqlConnectionStringBuilder Settings { get; }

        /// <summary>
        /// Contains the connection string returned to the user from <see cref="NpgsqlConnection.ConnectionString"/>
        /// after the connection has been opened. Does not contain the password unless Persist Security Info=true.
        /// </summary>
        internal string UserFacingConnectionString { get; }

        readonly int _max;
        readonly int _min;

        /// <summary>
        /// The total number of physical connections in existence, whether idle or busy out of the pool.
        /// </summary>
        /// <remarks>
        /// Internal for tests only
        /// </remarks>
        internal int Total;

        [ItemCanBeNull]
        readonly NpgsqlConnector[] _idle;

        readonly ConcurrentQueue<(TaskCompletionSource<NpgsqlConnector> TaskCompletionSource, bool IsAsync)> _waiting;
        int _waitingCount;

        /// <summary>
        /// Incremented every time this pool is cleared via <see cref="NpgsqlConnection.ClearPool"/> or
        /// <see cref="NpgsqlConnection.ClearAllPools"/>. Allows us to identify connections which were
        /// created before the clear.
        /// </summary>
        int _clearCounter;

        [CanBeNull]
        Timer _pruningTimer;
        readonly TimeSpan _pruningInterval;

        /// <summary>
        /// Maximum number of possible connections in any pool.
        /// </summary>
        internal const int PoolSizeLimit = 1024;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        #endregion

        internal ConnectorPool(NpgsqlConnectionStringBuilder settings, string connString)
        {
            if (settings.MaxPoolSize < settings.MinPoolSize)
                throw new ArgumentException($"Connection can't have MaxPoolSize {settings.MaxPoolSize} under MinPoolSize {settings.MinPoolSize}");

            Settings = settings;

            _max = settings.MaxPoolSize;
            _min = settings.MinPoolSize;

            UserFacingConnectionString = settings.PersistSecurityInfo
                ? connString
                : settings.ToStringWithoutPassword();

            _pruningInterval = TimeSpan.FromSeconds(Settings.ConnectionPruningInterval);
            _idle = new NpgsqlConnector[_max];
            _waiting = new ConcurrentQueue<(TaskCompletionSource<NpgsqlConnector> TaskCompletionSource, bool IsAsync)>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryAllocateFast(NpgsqlConnection conn, out NpgsqlConnector connector)
        {
            Counters.SoftConnectsPerSecond.Increment();

            // We start scanning for an idle connector in "random" places in the array, to avoid
            // too much interlocked operations "contention" at the beginning.
            var start = Thread.CurrentThread.ManagedThreadId % _max;

            for (var i = 0; i < _max; i++)
            {
                var index = (start + i) % _max;

                // First check without an Interlocked operation, it's faster
                if (_idle[index] == null)
                    continue;

                // If we saw a connector in this slot, atomically exchange it with a null.
                // Either we get a connector out which we can use, or we get null because
                // someone has taken it in the meanwhile. Either way put a null in its place.
                connector = Interlocked.Exchange(ref _idle[index], null);
                if (connector == null)
                    continue;

                Counters.NumberOfFreeConnections.Decrement();

                // An connector could be broken because of a keepalive that occurred while it was
                // idling in the pool
                // TODO: Consider removing the pool from the keepalive code. The following branch is simply irrelevant
                // if keepalive isn't turned on.
                if (connector.IsBroken)
                {
                    CloseConnector(connector);
                    continue;
                }

                connector.Connection = conn;
                Counters.NumberOfActiveConnections.Increment();
                return true;
            }

            connector = null;
            return false;
        }

        internal async Task<NpgsqlConnector> AllocateLong(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken)
        {
            // No idle connector was found in the pool.
            // We now loop until one of two things happen:
            // 1. The pool isn't at max capacity and we can create a new physical connection.
            // 2. The pool is at maximum capacity, so we enqueue an open attempt into the waiting queue, so that
            // the next release will unblock it.
            while (true)
            {
                var oldTotal = Total;
                if (oldTotal >= _max) // Pool is exhausted, wait for a close
                {
                    // The moment _waitingCount becomes non-zero, release attempts will block on the waiting queue and
                    // not go through to the idle list. This prevents a race condition where a connector makes it
                    // into the idle list but we're stuck in the waiting list until we time out.
                    Interlocked.Increment(ref _waitingCount);

                    try
                    {
                        // A connector may have slipped into the idle list before we increased the waiting count.
                        // Pass over the idle list once again
                        if (TryAllocateFast(conn, out var connector))
                        {
                            Interlocked.Decrement(ref _waitingCount);
                            return connector;
                        }

                        // We now know that the idle list is empty and will stay empty. Enqueue an open attempt
                        // into the waiting queue so that the next release attempt will unblock us.
                        // TODO: Async cancellation
                        var tcs = new TaskCompletionSource<NpgsqlConnector>();
                        _waiting.Enqueue((tcs, async));

                        try
                        {
                            if (async)
                            {
                                if (timeout.IsSet)
                                {
                                    var timeLeft = timeout.TimeLeft;
                                    if (timeLeft <= TimeSpan.Zero || tcs.Task != await Task.WhenAny(tcs.Task, Task.Delay(timeLeft)))
                                        throw new NpgsqlException($"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {Settings.Timeout} seconds)");
                                }
                                else
                                    await tcs.Task;
                            }
                            else
                            {
                                if (timeout.IsSet)
                                {
                                    var timeLeft = timeout.TimeLeft;
                                    if (timeLeft <= TimeSpan.Zero || !tcs.Task.Wait(timeLeft))
                                        throw new NpgsqlException($"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) or Timeout (currently {Settings.Timeout} seconds)");
                                }
                                else
                                    tcs.Task.Wait();
                            }
                        }
                        catch
                        {
                            // We're here if the timeout expired or the cancellation token was triggered.
                            // Transition our Task to cancelled, so that the next time someone releases
                            // a connection they'll skip over it.
                            if (tcs.TrySetCanceled())
                                throw;
                            // If we've failed to cancel, someone has released a connection in the meantime
                            // and we're good to go.
                        }

                        Debug.Assert(tcs.Task.IsCompleted);
                        connector = tcs.Task.Result;
                        // Note that we don't update counters or any state since the connector is being
                        // handed off from one open connection to another.
                        connector.Connection = conn;
                        return connector;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _waitingCount);
                    }
                }

                // Try to "allocate" a slot for a new physical connection. If we increase the total connections and
                // are still under the max, we can create a new physical connection. Otherwise loop back again.
                if (Interlocked.CompareExchange(ref Total, oldTotal + 1, oldTotal) == oldTotal)
                {
                    var connector = new NpgsqlConnector(conn) { ClearCounter = _clearCounter };
                    try
                    {
                        await connector.Open(timeout, async, cancellationToken);
                    }
                    catch
                    {
                        // Total has already been incremented, decrement it back
                        Interlocked.Decrement(ref Total);
                        throw;
                    }

                    Counters.NumberOfActiveConnections.Increment();
                    Counters.NumberOfPooledConnections.Increment();

                    // Start the pruning timer if we're above MinPoolSize
                    if (_pruningTimer == null && Total > _min)
                    {
                        var newPruningTimer = new Timer(PruneIdleConnectors);
                        if (Interlocked.CompareExchange(ref _pruningTimer, newPruningTimer, null) == null)
                            newPruningTimer.Change(_pruningInterval, _pruningInterval);
                        else
                        {
                            // Someone beat us to it
                            newPruningTimer.Dispose();
                        }
                    }

                    return connector;
                }
            }

            // Cannot be here
        }

        internal void Release(NpgsqlConnector connector)
        {
            Counters.SoftDisconnectsPerSecond.Increment();
            Counters.NumberOfActiveConnections.Decrement();

            // If Clear/ClearAll has been been called since this connector was first opened,
            // throw it away. The same if it's broken (in which case CloseConnector is only
            // used to update state/perf counter).
            if (connector.ClearCounter < _clearCounter || connector.IsBroken)
            {
                CloseConnector(connector);
                return;
            }

            connector.Reset();

            // If there are any pending open attempts in progress hand the connector off to
            // them directly.
            while (_waitingCount > 0)
            {
                if (!_waiting.TryDequeue(out var waitingOpenAttempt))
                {
                    // _waitingCount has been increased, but there's nothing in the queue yet - someone is in the
                    // process of enqueuing an open attempt. Wait and retry.
                    Thread.Sleep(5);
                    continue;
                }

                var tcs = waitingOpenAttempt.TaskCompletionSource;

                // We have a pending open attempt. "Complete" it, handing off the connector.
                if (waitingOpenAttempt.IsAsync)
                {
                    // If the waiting open attempt is asynchronous (i.e. OpenAsync()), we can't simply
                    // call SetResult on its TaskCompletionSource, since it would execute the open's
                    // continuation in our thread (the closing thread). Instead we schedule the completion
                    // to run in the TP

                    // We copy tcs2 and especially connector2 to avoid allocations caused by the closure, see
                    // http://stackoverflow.com/questions/41507166/closure-heap-allocation-happening-at-start-of-method
                    var tcs2 = tcs;
                    var connector2 = connector;

                    Task.Run(() =>
                    {
                        if (!tcs2.TrySetResult(connector2))
                        {
                            // Race condition: the waiter timed out between our IsCanceled check above and here
                            // Recursively call Release again, this will dequeue another open attempt and retry.
                            Debug.Assert(tcs2.Task.IsCanceled);
                            Release(connector2);
                        }
                    });
                }
                else if (!tcs.TrySetResult(connector))
                {
                    // Race condition: the waiter timed out between our IsCanceled check above and here
                    // Recursively call Release again, this will dequeue another open attempt and retry.
                    Debug.Assert(tcs.Task.IsCanceled);
                    continue;
                }

                return;
            }

            // There were no pending open attempts, simply place the connector back in the idle list
            for (var i = 0; i < _idle.Length; i++)
            {
                if (Interlocked.CompareExchange(ref _idle[i], connector, null) == null)
                {
                    Counters.NumberOfFreeConnections.Increment();
                    connector.ReleaseTimestamp = DateTime.UtcNow;
                    return;
                }
            }

            // Should not be here
            Log.Error("The idle list was full when releasing, there are more than MaxPoolSize connectors! Please file an issue.");
            CloseConnector(connector);
        }

        void CloseConnector(NpgsqlConnector connector)
        {
            try
            {
                connector.Close();
            }
            catch (Exception e)
            {
                Log.Warn("Exception while closing outdated connector", e, connector.Id);
            }

            Interlocked.Decrement(ref Total);
            Counters.NumberOfPooledConnections.Decrement();

            while (_pruningTimer != null && Total <= _min)
            {
                var oldTimer = _pruningTimer;
                if (Interlocked.CompareExchange(ref _pruningTimer, null, oldTimer) == oldTimer)
                {
                    oldTimer.Dispose();
                    break;
                }
            }
        }

#pragma warning disable CA1801 // Review unused parameters
        void PruneIdleConnectors(object state)
#pragma warning restore CA1801 // Review unused parameters
        {
            var now = DateTime.UtcNow;
            var idleLifetime = Settings.ConnectionIdleLifetime;

            for (var i = 0; i < _idle.Length; i++)
            {
                if (Total <= _min)
                    return;

                var connector = _idle[i];
                if (connector == null || (now - connector.ReleaseTimestamp).TotalSeconds < idleLifetime)
                    continue;
                if (Interlocked.CompareExchange(ref _idle[i], null, connector) == connector)
                    CloseConnector(connector);
            }
        }

        internal void Clear()
        {
            var toClose = new List<NpgsqlConnector>(_max);

            for (var i = 0; i < _idle.Length; i++)
            {
                var connector = Interlocked.Exchange(ref _idle[i], null);
                if (connector != null)
                {
                    toClose.Add(connector);
                }
            }

            foreach (var connector in toClose)
                CloseConnector(connector);

            _clearCounter++;
        }

        #region Pending Enlisted Connections

        internal void AddPendingEnlistedConnector(NpgsqlConnector connector, Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    list = _pendingEnlistedConnectors[transaction] = new List<NpgsqlConnector>();
                list.Add(connector);
            }
        }

        internal void TryRemovePendingEnlistedConnector(NpgsqlConnector connector, Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    return;
                list.Remove(connector);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
            }
        }

        [CanBeNull]
        internal NpgsqlConnector TryAllocateEnlistedPending(Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    return null;
                var connector = list[list.Count - 1];
                list.RemoveAt(list.Count - 1);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
                return connector;
            }
        }

        // Note that while the dictionary is threadsafe, we assume that the lists it contains don't need to be
        // (i.e. access to connectors of a specific transaction won't be concurrent)
        readonly Dictionary<Transaction, List<NpgsqlConnector>> _pendingEnlistedConnectors
            = new Dictionary<Transaction, List<NpgsqlConnector>>();

        #endregion

        public void Dispose() => _pruningTimer?.Dispose();

        public override string ToString() => $"[{Total} total, {_idle.Count(i => i != null)} idle, {_waiting.Count} waiting]";

        /// <summary>
        /// Returns the number of idle connector in the pool, for testing purposes only.
        /// </summary>
        internal int IdleCount => _idle.Count(i => i != null);
    }
}
