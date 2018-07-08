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
using Npgsql.Util;

namespace Npgsql.FrontendMessages
{
    /// <summary>
    /// A simple query message.
    /// </summary>
    class QueryMessage : SimpleFrontendMessage
    {
        readonly Encoding _encoding;
        string _query;

        const byte Code = (byte)'Q';

        internal QueryMessage(Encoding encoding)
        {
            _encoding = encoding;
        }

        internal QueryMessage Populate(string query)
        {
            Debug.Assert(query != null);

            _query = query;
            _length = 1 + 4 + _encoding.GetByteCount(_query) + 1;
            return this;
        }

        internal override int Length => _length;
        int _length;

        internal override void WriteFully(Span<byte> span)
        {
            span[0] = Code;
            span = span.Slice(1);

            BinaryPrimitives.WriteInt32BigEndian(span, _length - 1);
            span = span.Slice(4);

            span.WriteNullTerminatedString(_encoding, _query);
        }

        public override string ToString() => $"[Query={_query}]";
    }
}
