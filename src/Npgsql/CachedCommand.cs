using System;
using System.Collections.Generic;
using System.Diagnostics;
using JetBrains.Annotations;
using NpgsqlTypes;

namespace Npgsql
{
    class CachedCommand
    {
        internal CachedCommand(string sql, List<NpgsqlStatement> statements)
        {
            Sql = sql;
            Statements = statements;
        }

        internal string Sql { get; }
        internal List<NpgsqlStatement> Statements { get; }

        /// <summary>
        /// Whether all statements referenced by this cached command are prepared.
        /// </summary>
        internal bool IsPrepared { get; set; }

        internal bool IsAutoPrepared { get; set; }

        internal int Usages;

        internal DateTime LastUsed { get; set; }

        /// <summary>
        /// Contains the parameter types for a prepared statement, for overloaded cases (same SQL, different param types)
        /// Only populated after the statement has been prepared (i.e. null for candidates).
        /// </summary>
        [CanBeNull]
        internal NpgsqlDbType[] ParamTypes { get; private set; }

        /// <summary>
        /// Count of NpgsqlCommand instances referencing this <see cref="CachedCommand"/>.
        /// </summary>
        internal int RefCount;

        static readonly NpgsqlDbType[] EmptyParamTypes = new NpgsqlDbType[0];

        internal bool DoParametersMatch(IList<NpgsqlParameter> parameters)
        {
            Debug.Assert(ParamTypes != null);
            if (ParamTypes.Length != parameters.Count)
                return false;
            for (var i = 0; i < ParamTypes.Length; i++)
                if (ParamTypes[i] != parameters[i].NpgsqlDbType)
                    return false;
            return true;
        }

        internal void SetParamTypes(IList<NpgsqlParameter> parameters)
        {
            Debug.Assert(ParamTypes == null);
            if (parameters.Count == 0)
                ParamTypes = EmptyParamTypes;
            ParamTypes = new NpgsqlDbType[parameters.Count];
            for (var i = 0; i < parameters.Count; i++)
                ParamTypes[i] = parameters[i].NpgsqlDbType;
        }

        public override string ToString() => Sql;
    }
}
