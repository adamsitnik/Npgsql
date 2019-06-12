using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Npgsql.Util
{
    /// <summary>
    /// A dictionary facade that tracks changes and reset back to a snapshot. Does not support read operations, which
    /// must be made directly against the underlying dictionary to avoid a perf penalty.
    /// Nulls as values are not supported.
    /// </summary>
    class ResettableDictionaryFacade<TKey, TValue> : IDictionary<TKey, TValue>
        where TValue : class
    {
        readonly IDictionary<TKey, TValue> _underlying;
        List<(TKey, TValue?)>? _log;

        public ResettableDictionaryFacade(IDictionary<TKey, TValue> dictionary)
            => _underlying = dictionary;

        public void TakeSnapshot()
            => _log = _log == null
                ? new List<(TKey, TValue?)>()
                : throw new InvalidOperationException("A snapshot has already been taken");

        public void DropSnapshot()
            => _log = null;

        public void Reset()
        {
            if (_log == null)
                throw new InvalidOperationException("A snapshot hasn't been taken");

            foreach (var (key, value) in _log)
            {
                if (value == null)
                    _underlying.Remove(key);
                else
                    _underlying[key] = value;
            }
            _log.Clear();
        }

        #region Write

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            AddEntryToLog(item.Key);
            _underlying.Add(item);
        }

        public void Clear()
        {
            _log?.AddRange(_underlying.Select(kv => (kv.Key, (TValue?)kv.Value)));
            _underlying.Clear();
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            AddEntryToLog(item.Key);
            return _underlying.Remove(item);
        }

        public void Add(TKey key, TValue value)
        {
            AddEntryToLog(key);
            _underlying.Add(key, value);
        }

        public bool Remove(TKey key)
        {
            AddEntryToLog(key);
            return _underlying.Remove(key);
        }

        public TValue this[TKey key]
        {
            get => throw ReadNotSupported();
            set
            {
                AddEntryToLog(key);
                _underlying[key] = value;
            }
        }

        void AddEntryToLog(TKey key)
            => _log?.Add((key, _underlying.TryGetValue(key, out var previousValue) ? previousValue : null));

        #endregion Write

        #region Read

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => throw ReadNotSupported();
        IEnumerator IEnumerable.GetEnumerator() => throw ReadNotSupported();
        public bool Contains(KeyValuePair<TKey, TValue> item) => throw ReadNotSupported();
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) => throw ReadNotSupported();
        public int Count => throw ReadNotSupported();
        public bool IsReadOnly => throw ReadNotSupported();
        public bool ContainsKey(TKey key) => throw ReadNotSupported();
        public bool TryGetValue(TKey key, out TValue value) => throw ReadNotSupported();
        public ICollection<TKey> Keys => throw ReadNotSupported();
        public ICollection<TValue> Values => throw ReadNotSupported();

        NotSupportedException ReadNotSupported() =>
            new NotSupportedException("Reading is not supported on this facade, access the underlying dictionary directly.");

        #endregion Read
    }
}
