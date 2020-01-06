using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Transactions;
using Npgsql.Logging;
using Npgsql.Util;

namespace Npgsql
{
    sealed class ConnectorPool
    {
        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(ConnectorPool));

        internal NpgsqlConnectionStringBuilder Settings { get; }

        /// <summary>
        /// Contains the connection string returned to the user from <see cref="NpgsqlConnection.ConnectionString"/>
        /// after the connection has been opened. Does not contain the password unless Persist Security Info=true.
        /// </summary>
        internal string UserFacingConnectionString { get; }

        readonly int _max;
        readonly int _min;
        volatile int _numConnectors;
        volatile int _idle;

        readonly ChannelReader<NpgsqlConnector> _idleReader;
        readonly ChannelWriter<NpgsqlConnector> _idleWriter;

        /// <summary>
        /// Incremented every time this pool is cleared via <see cref="NpgsqlConnection.ClearPool"/> or
        /// <see cref="NpgsqlConnection.ClearAllPools"/>. Allows us to identify connections which were
        /// created before the clear.
        /// </summary>
        volatile int _clearCounter;

        static readonly TimerCallback PruningTimerCallback = PruneIdleConnectors;
        readonly Timer _pruningTimer;
        readonly TimeSpan _pruningSamplingInterval;
        readonly int _pruningSampleSize;
        readonly int[] _pruningSamples;
        readonly int _pruningMedianIndex;
        volatile bool _pruningTimerEnabled;
        int _pruningSampleIndex;

        // Note that while the dictionary is protected by locking, we assume that the lists it contains don't need to be
        // (i.e. access to connectors of a specific transaction won't be concurrent)
        readonly Dictionary<Transaction, List<NpgsqlConnector>> _pendingEnlistedConnectors
            = new Dictionary<Transaction, List<NpgsqlConnector>>();

        internal (int Total, int Idle, int Busy) Statistics
        {
            get
            {
                var numConnectors = _numConnectors;
                var idle = _idle;
                return (numConnectors, idle, numConnectors - idle);
            }
        }

        internal ConnectorPool(NpgsqlConnectionStringBuilder settings, string connString)
        {
            if (settings.MaxPoolSize < settings.MinPoolSize)
                throw new ArgumentException($"Connection can't have MaxPoolSize {settings.MaxPoolSize} under MinPoolSize {settings.MinPoolSize}");

            // We enforce Max Pool Size anyway, so no need to to create a bounded channel (which is less efficient)
            var idleChannel = Channel.CreateUnbounded<NpgsqlConnector>();
            _idleReader = idleChannel.Reader;
            _idleWriter = idleChannel.Writer;

            _max = settings.MaxPoolSize;
            _min = settings.MinPoolSize;

            UserFacingConnectionString = settings.PersistSecurityInfo
                ? connString
                : settings.ToStringWithoutPassword();

            Settings = settings;

            if (settings.ConnectionPruningInterval == 0)
                throw new ArgumentException("ConnectionPruningInterval can't be 0.");
            var connectionIdleLifetime = TimeSpan.FromSeconds(settings.ConnectionIdleLifetime);
            var pruningSamplingInterval = TimeSpan.FromSeconds(settings.ConnectionPruningInterval);
            if (connectionIdleLifetime < pruningSamplingInterval)
                throw new ArgumentException($"Connection can't have ConnectionIdleLifetime {connectionIdleLifetime} under ConnectionPruningInterval {_pruningSamplingInterval}");

            _pruningTimer = new Timer(PruningTimerCallback, this, Timeout.Infinite, Timeout.Infinite);
            _pruningSampleSize = DivideRoundingUp(connectionIdleLifetime.Seconds, pruningSamplingInterval.Seconds);
            _pruningMedianIndex = DivideRoundingUp(_pruningSampleSize, 2) - 1; // - 1 to go from length to index
            _pruningSamplingInterval = pruningSamplingInterval;
            _pruningSamples = new int[_pruningSampleSize];
            _pruningTimerEnabled = false;
        }

        internal ValueTask<NpgsqlConnector> Rent(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async,
            CancellationToken cancellationToken)
        {
            Counters.SoftConnectsPerSecond.Increment();

            if (TryGetIdleConnector(out var connector))
            {
                connector.Connection = conn;
                return new ValueTask<NpgsqlConnector>(connector);
            }

            return RentAsync(conn, timeout, async, cancellationToken);

            async ValueTask<NpgsqlConnector> RentAsync(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async,
                CancellationToken cancellationToken)
            {
                // TODO: If we're synchronous, use SingleThreadSynchronizationContext to not schedule completions
                // on the thread pool (standard sync-over-async TP pseudo-deadlock)

                // No idle connector was found in the pool.
                // As long as we're under max capacity, attempt to increase the connector count and open a new connection.
                for (var numConnectors = _numConnectors; numConnectors < _max; numConnectors = _numConnectors)
                {
                    // Note that we purposefully don't use SpinWait for this: https://github.com/dotnet/coreclr/pull/21437
                    if (Interlocked.CompareExchange(ref _numConnectors, numConnectors + 1, numConnectors) !=
                        numConnectors)
                        continue;

                    try
                    {
                        // We've managed to increase the open counter, open a physical connections.
                        connector = new NpgsqlConnector(conn) { ClearCounter = _clearCounter };
                        await connector.Open(timeout, async, cancellationToken);
                    }
                    catch
                    {
                        // Physical open failed, decrement the open and busy counter back down.
                        conn.Connector = null;
                        Interlocked.Decrement(ref _numConnectors);
                        throw;
                    }

                    // Only start pruning if it was this thread that incremented open count past _min.
                    if (numConnectors == _min)
                        EnablePruning();
                    Counters.NumberOfPooledConnections.Increment();
                    Counters.NumberOfActiveConnections.Increment();

                    return connector;
                }

                // We're at max capacity. Asynchronously wait on the idle channel.

                // TODO: Potential issue: we only check to create new connections once above. In theory we could have
                // many attempts waiting on the idle channel forever, since all connections were broken by some network
                // event. Pretty sure this issue exists in the old lock-free implementation too, think about it (it would
                // be enough to retry the physical creation above).

                var timeoutSource = new CancellationTokenSource(timeout.TimeLeft);
                var timeoutToken = timeoutSource.Token;
                using var _ = cancellationToken.Register(cts => ((CancellationTokenSource)cts!).Cancel(), timeoutSource);
                try
                {
                    while (await _idleReader.WaitToReadAsync(timeoutToken))
                    {
                        if (TryGetIdleConnector(out connector))
                        {
                            connector.Connection = conn;
                            return connector;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    throw new NpgsqlException(
                        $"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) " +
                        $"or Timeout (currently {Settings.Timeout} seconds)");
                }

                // TODO: The channel has been completed, the pool is being disposed. Does this actually occur?
                throw new NpgsqlException("The connection pool has been shut down.");
            }
        }

        bool TryGetIdleConnector([NotNullWhen(true)] out NpgsqlConnector connector)
        {
            while (_idleReader.TryRead(out connector))
            {
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

                Counters.NumberOfActiveConnections.Increment();
                Interlocked.Decrement(ref _idle);
                return true;
            }

            return false;
        }

        internal void Return(NpgsqlConnector connector)
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

            // Order is important since we have synchronous completions on the channel.
            Interlocked.Increment(ref _idle);
            var written = _idleWriter.TryWrite(connector);
            Debug.Assert(written);
        }

        internal void Clear()
        {
            Interlocked.Increment(ref _clearCounter);
            while (TryGetIdleConnector(out var connector))
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

            Counters.NumberOfPooledConnections.Decrement();

            var numConnectors = Interlocked.Decrement(ref _numConnectors);
            Debug.Assert(numConnectors >= 0);
            // Only turn off the timer one time, when it was this Close that brought Open back to _min.
            if (numConnectors == _min)
                DisablePruning();
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

        internal bool TryRentEnlistedPending(
            NpgsqlConnection connection,
            Transaction transaction,
            [NotNullWhen(true)] out NpgsqlConnector? connector)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                {
                    connector = null;
                    return false;
                }
                connector = list[list.Count - 1];
                list.RemoveAt(list.Count - 1);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
                connector.Connection = connection;
                return true;
            }
        }

        #endregion

        #region Pruning

        // Manual reactivation of timer happens in callback
        void EnablePruning()
        {
            lock (_pruningTimer)
            {
                _pruningTimerEnabled = true;
                _pruningTimer.Change(_pruningSamplingInterval, Timeout.InfiniteTimeSpan);
            }
        }

        void DisablePruning()
        {
            lock (_pruningTimer)
            {
                _pruningTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _pruningSampleIndex = 0;
                _pruningTimerEnabled = false;
            }
        }

        static void PruneIdleConnectors(object? state)
        {
            var pool = (ConnectorPool)state!;
            var samples = pool._pruningSamples;
            int toPrune;
            lock (pool._pruningTimer)
            {
                // Check if we might have been contending with DisablePruning.
                if (!pool._pruningTimerEnabled)
                    return;

                var sampleIndex = pool._pruningSampleIndex;
                if (sampleIndex < pool._pruningSampleSize)
                {
                    samples[sampleIndex] = pool._idle;
                    pool._pruningSampleIndex = sampleIndex + 1;
                    pool._pruningTimer.Change(pool._pruningSamplingInterval, Timeout.InfiniteTimeSpan);
                    return;
                }

                // Calculate median value for pruning, reset index and timer, and release the lock.
                Array.Sort(samples);
                toPrune = samples[pool._pruningMedianIndex];
                pool._pruningSampleIndex = 0;
                pool._pruningTimer.Change(pool._pruningSamplingInterval, Timeout.InfiniteTimeSpan);
            }

            for (var i = 0; i < toPrune && pool._numConnectors > pool._min; i++)
            {
                if (!pool.TryGetIdleConnector(out var connector))
                    return;

                pool.CloseConnector(connector);
            }
        }

        static int DivideRoundingUp(int value, int divisor) => 1 + (value - 1) / divisor;

        #endregion
    }
}
