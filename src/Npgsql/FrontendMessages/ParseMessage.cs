#region License
// The PostgreSQL License
//
// Copyright (C) 2018 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.TypeMapping;
using Npgsql.Util;

namespace Npgsql.FrontendMessages
{
    class ParseMessage : FrontendMessage
    {
        /// <summary>
        /// The query string to be parsed.
        /// </summary>
        string Query { get; set; }

        /// <summary>
        /// The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
        /// </summary>
        string Statement { get; set; }

        // ReSharper disable once InconsistentNaming
        List<uint> ParameterTypeOIDs { get; }

        readonly Encoding _encoding;

        const byte Code = (byte)'P';

        internal ParseMessage(Encoding encoding)
        {
            _encoding = encoding;
            ParameterTypeOIDs = new List<uint>();
        }

        internal ParseMessage Populate(string sql, string statementName, List<NpgsqlParameter> inputParameters, ConnectorTypeMapper typeMapper)
        {
            Populate(sql, statementName);
            foreach (var inputParam in inputParameters)
            {
                inputParam.ResolveHandler(typeMapper);
                ParameterTypeOIDs.Add(inputParam.Handler.PostgresType.OID);
            }
            return this;
        }

        internal ParseMessage Populate(string sql, string statementName)
        {
            ParameterTypeOIDs.Clear();
            Query = sql;
            Statement = statementName;
            return this;
        }

        internal override Task Write(PipeWriter writer, bool async)
        {
            Debug.Assert(Statement != null && Statement.All(c => c < 128));

            var queryByteLen = _encoding.GetByteCount(Query);
            var messageLength =
                1 +                         // Message code
                4 +                         // Length
                Statement.Length +
                1 +                         // Null terminator
                queryByteLen +
                1 +                         // Null terminator
                2 +                         // Number of parameters
                ParameterTypeOIDs.Count * 4;

            // TODO: We require a continuous buffer for the entire message, reconsider
            var span = writer.GetSpan(messageLength);

            span[0] = Code;
            span = span.Slice(1);

            BinaryPrimitives.WriteInt32BigEndian(span, messageLength - 1);
            span = span.Slice(4);

            span = span.Slice(span.WriteNullTerminatedString(Encoding.ASCII, Statement));
            span = span.Slice(span.WriteNullTerminatedString(_encoding, Statement));

            BinaryPrimitives.WriteInt16BigEndian(span, (short)ParameterTypeOIDs.Count);
            span = span.Slice(2);

            foreach (var t in ParameterTypeOIDs)
            {
                BinaryPrimitives.WriteInt32BigEndian(span, (int)t);
                span = span.Slice(2);
            }

            writer.Advance(messageLength);
            return Task.CompletedTask;
        }

        public override string ToString()
            => $"[Parse(Statement={Statement},NumParams={ParameterTypeOIDs.Count}]";
    }
}
