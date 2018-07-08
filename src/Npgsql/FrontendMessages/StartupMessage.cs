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
using System.Text;
using Npgsql.Util;

namespace Npgsql.FrontendMessages
{
    class StartupMessage : SimpleFrontendMessage
    {
        readonly Dictionary<string, string> _parameters = new Dictionary<string, string>();
        int _length;

        const int ProtocolVersion3 = 3 << 16; // 196608

        internal Encoding Encoding { get; }

        internal StartupMessage(Encoding encoding)
        {
            Encoding = encoding;
        }

        internal string this[string key]
        {
            set => _parameters[key] = value;
        }

        internal override int Length
        {
            get
            {
                _length = 4 + // len
                          4 + // protocol version
                          1;  // trailing zero byte

                foreach (var kvp in _parameters)
                    _length += PGUtil.UTF8Encoding.GetByteCount(kvp.Key) + 1 +
                               PGUtil.UTF8Encoding.GetByteCount(kvp.Value) + 1;
                return _length;
            }
        }

        internal override void WriteFully(Span<byte> span)
        {
            BinaryPrimitives.WriteInt32BigEndian(span, _length);
            span = span.Slice(4);
            BinaryPrimitives.WriteInt32BigEndian(span, ProtocolVersion3);
            span = span.Slice(4);

            foreach (var kv in _parameters)
            {
                span = span.Slice(span.WriteNullTerminatedString(Encoding, kv.Key));
                span = span.Slice(span.WriteNullTerminatedString(Encoding, kv.Value));
            }

            span[0] = 0;
        }
    }
}
