using System;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable 1573

#pragma warning disable 1591

namespace Npgsql.Util
{
    public sealed class ReusableAwaiter<T> : INotifyCompletion
    {
        Action _continuation;
        T _result;
        Exception _exception;

        int _isCompleted;

        public bool IsCompleted => Volatile.Read(ref _isCompleted) == 1;

        public T GetResult() => _exception != null ? throw _exception : _result;

        public void OnCompleted(Action continuation)
            => _continuation ??= continuation ?? throw new InvalidOperationException("This ReusableAwaiter instance has already been listened");

        public bool TrySetResult(T result)
        {
            if (Interlocked.CompareExchange(ref _isCompleted, 1, 0) == 1)
                return false;

            _result = result;

            var continuation = _continuation;
            if (continuation != null)
                continuation();
            return true;
        }

        public bool TrySetException(Exception exception)
        {
            if (Interlocked.CompareExchange(ref _isCompleted, 1, 0) == 1)
                return false;

            _exception = exception;

            var continuation = _continuation;
            if (continuation != null)
                continuation();
            return true;
        }

        public void Reset()
        {
            _result = default;
            _continuation = null;
            _exception = null;
            _isCompleted = 0;
        }

        public ReusableAwaiter<T> GetAwaiter() => this;
    }
}
