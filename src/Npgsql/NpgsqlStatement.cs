using System.Collections.Generic;
using System.Diagnostics;
using JetBrains.Annotations;
using Npgsql.BackendMessages;

namespace Npgsql
{
    /// <summary>
    /// Represents a single SQL statement within Npgsql.
    ///
    /// Instances aren't constructed directly; users should construct an <see cref="NpgsqlCommand"/>
    /// object and populate its <see cref="NpgsqlCommand.CommandText"/> property as in standard ADO.NET.
    /// Npgsql will analyze that property and constructed instances of <see cref="NpgsqlStatement"/>
    /// internally.
    ///
    /// Users can retrieve instances from <see cref="NpgsqlDataReader.Statements"/>
    /// and access information about statement execution (e.g. affected rows).
    /// </summary>
    public sealed class NpgsqlStatement
    {
        /// <summary>
        /// The SQL text of the statement.
        /// </summary>
        public string SQL { get; set; } = string.Empty;

        /// <summary>
        /// Specifies the type of query, e.g. SELECT.
        /// </summary>
        public StatementType StatementType { get; internal set; }

        /// <summary>
        /// The number of rows affected or retrieved.
        /// </summary>
        /// <remarks>
        /// See the command tag in the CommandComplete message,
        /// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
        /// </remarks>
        public uint Rows { get; internal set; }

        /// <summary>
        /// For an INSERT, the object ID of the inserted row if <see cref="Rows"/> is 1 and
        /// the target table has OIDs; otherwise 0.
        /// </summary>
        public uint OID { get; internal set; }

        /// <summary>
        /// The input parameters sent with this statement.
        /// </summary>
        public List<NpgsqlParameter> InputParameters { get; } = new List<NpgsqlParameter>();

        /// <summary>
        /// The PostgreSQL statement name. If null, this statement will run unprepared.
        /// </summary>
        internal string Name;

        /// <summary>
        /// How many instances of <see cref="CachedCommand"/> are referencing this prepared statement.
        /// </summary>
        internal int RefCount;

        [CanBeNull]
        internal RowDescriptionMessage Description;

        internal PreparedState State { get; set; }

        internal bool IsPrepared => State == PreparedState.Prepared;

        /// <summary>
        /// If we've reached the limit of prepared statements, this references the LRU command that will be unprepared
        /// to make room for us. Note that this command may reference several other statements, no necessarily one.
        /// </summary>
        [CanBeNull]
        internal CachedCommand CommandBeingReplaced;

        internal void Reset()
        {
            SQL = string.Empty;
            Name = null;
            State = PreparedState.NotPrepared;
            StatementType = StatementType.Select;
            Description = null;
            Rows = 0;
            OID = 0;
            InputParameters.Clear();
        }

        internal void ApplyCommandComplete(CommandCompleteMessage msg)
        {
            StatementType = msg.StatementType;
            Rows = msg.Rows;
            OID = msg.OID;
        }

        /// <summary>
        /// Returns the SQL text of the statement.
        /// </summary>
        public override string ToString() => SQL ?? "<none>";
    }

    enum PreparedState
    {
        /// <summary>
        /// The statement hasn't been prepared yet, nor is it in the process of being prepared.
        /// </summary>
        NotPrepared,

        /// <summary>
        /// The statement has been selected for preparation, but the preparation hasn't started yet.
        /// This is a temporary state that only occurs during preparation.
        /// Specifically, it means that a Parse message for the statement has already been written to the write buffer.
        /// </summary>
        ToBePrepared,

        /// <summary>
        /// The statement is in the process of being prepared. This is a temporary state that only occurs during
        /// preparation. Specifically, it means that a Parse message for the statement has already been written
        /// to the write buffer, but confirmation of the preparation (ParseComplete) hasn't yet been received from
        /// the server.
        /// </summary>
        BeingPrepared,

        /// <summary>
        /// The statement has been fully prepared and can be executed.
        /// </summary>
        Prepared,

        /// <summary>
        /// The statement is in the process of being unprepared. This is a temporary state that only occurs during
        /// unpreparation. Specifically, it means that a Close message for the statement has already been written
        /// to the write buffer.
        /// </summary>
        BeingUnprepared
    }
}
