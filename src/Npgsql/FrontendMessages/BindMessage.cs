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
using JetBrains.Annotations;
using Npgsql.Util;

namespace Npgsql.FrontendMessages
{
    class BindMessage : FrontendMessage
    {
        /// <summary>
        /// The name of the destination portal (an empty string selects the unnamed portal).
        /// </summary>
        string Portal { get; set; }

        /// <summary>
        /// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        /// </summary>
        string Statement { get; set; }

        List<NpgsqlParameter> InputParameters { get; set; }
        internal List<FormatCode> ResultFormatCodes { get; private set; }
        internal bool AllResultTypesAreUnknown { get; set; }
        [CanBeNull]
        internal bool[] UnknownResultTypeList { get; set; }

        const byte Code = (byte)'B';

        internal BindMessage Populate(List<NpgsqlParameter> inputParameters, string portal = "", string statement = "")
        {
            Debug.Assert(inputParameters != null && inputParameters.All(p => p.IsInputDirection));
            Debug.Assert(portal != null);
            Debug.Assert(statement != null);

            AllResultTypesAreUnknown = false;
            UnknownResultTypeList = null;
            Portal = portal;
            Statement = statement;
            InputParameters = inputParameters;
            return this;
        }

        internal override async Task Write(PipeWriter writer, bool async)
        {
            Debug.Assert(Statement != null && Statement.All(c => c < 128));
            Debug.Assert(Portal != null && Portal.All(c => c < 128));

            WriteHeader();

            foreach (var param in InputParameters)
            {
                param.LengthCache?.Rewind();
                await param.WriteWithLength(writer, async);
            }

            WriteTrailer();

            void WriteHeader()
            {
                var formatCodesSum = 0;
                var paramsLength = 0;
                foreach (var p in InputParameters)
                {
                    formatCodesSum += (int)p.FormatCode;
                    p.LengthCache?.Rewind();
                    paramsLength += p.ValidateAndGetLength();
                }

                var formatCodeListLength = formatCodesSum == 0 ? 0 : formatCodesSum == InputParameters.Count ? 1 : InputParameters.Count;

                var headerLength =
                    1 +                         // Message code
                    4 +                         // Message length
                    1 +                         // Portal is always empty (only a null terminator)
                    Statement.Length + 1 +
                    2 +                         // Number of parameter format codes that follow
                    2 * formatCodeListLength +  // List of format codes
                    2;                          // Number of parameters

                var messageLength = headerLength +
                    4 * InputParameters.Count +               // Parameter lengths
                    paramsLength +                            // Parameter values
                    2 +                                       // Number of result format codes
                    2 * (UnknownResultTypeList?.Length ?? 1); // Result format codes

                var span = writer.GetSpan(headerLength);

                span[0] = Code;
                span = span.Slice(1);

                BinaryPrimitives.WriteInt32BigEndian(span, messageLength - 1);
                span = span.Slice(4);

                Debug.Assert(Portal == string.Empty);
                span[0] = 0;  // Portal is always empty
                span = span.Slice(1);

                span = span.Slice(span.WriteNullTerminatedString(Encoding.ASCII, Statement));
                BinaryPrimitives.WriteInt32BigEndian(span, formatCodeListLength);

                // 0 length implicitly means all-text, 1 means all-binary, >1 means mix-and-match
                switch (formatCodeListLength)
                {
                case 0:   // All-text parameters, nothing needed
                    break;

                case 1:   // All-binary parameters (normal case)
                    BinaryPrimitives.WriteInt16BigEndian(span, (short)FormatCode.Binary);
                    span = span.Slice(2);
                    break;

                default:  // Mix-and-match, some text and some binary
                    foreach (var p in InputParameters)
                    {
                        BinaryPrimitives.WriteInt16BigEndian(span, (short)p.FormatCode);
                        span = span.Slice(2);
                    }
                    break;
                }

                BinaryPrimitives.WriteInt16BigEndian(span, (short)InputParameters.Count);

                writer.Advance(headerLength);
            }

            void WriteTrailer()
            {
                if (UnknownResultTypeList != null)
                {
                    throw new NotImplementedException();
#if NO
                if (buf.WriteSpaceLeft < 2 + UnknownResultTypeList.Length * 2)
                    await buf.Flush(async);
                buf.WriteInt16(UnknownResultTypeList.Length);
                foreach (var t in UnknownResultTypeList)
                    buf.WriteInt16(t ? 0 : 1);
#endif
                }
                else
                {
                    var span = writer.GetSpan(4);
                    BinaryPrimitives.WriteInt16BigEndian(span, 1);
                    span = span.Slice(2);
                    BinaryPrimitives.WriteInt16BigEndian(span, (short)(AllResultTypesAreUnknown ? 0 : 1));
                    writer.Advance(4);
                }
            }
        }

#if NO
        internal override async Task Write(NpgsqlWriteBuffer buf, bool async)
        {
            Debug.Assert(Statement != null && Statement.All(c => c < 128));
            Debug.Assert(Portal != null && Portal.All(c => c < 128));

            var headerLength =
                1 +                        // Message code
                4 +                        // Message length
                1 +                        // Portal is always empty (only a null terminator)
                Statement.Length + 1 +
                2;                         // Number of parameter format codes that follow

            if (buf.WriteSpaceLeft < headerLength)
            {
                Debug.Assert(buf.Size >= headerLength, "Buffer too small for Bind header");
                await buf.Flush(async);
            }

            var formatCodesSum = 0;
            var paramsLength = 0;
            foreach (var p in InputParameters)
            {
                formatCodesSum += (int)p.FormatCode;
                p.LengthCache?.Rewind();
                paramsLength += p.ValidateAndGetLength();
            }

            var formatCodeListLength = formatCodesSum == 0 ? 0 : formatCodesSum == InputParameters.Count ? 1 : InputParameters.Count;

            var messageLength = headerLength +
                2 * formatCodeListLength + // List of format codes
                2 +                         // Number of parameters
                4 * InputParameters.Count +                                     // Parameter lengths
                paramsLength +                                                  // Parameter values
                2 +                                                             // Number of result format codes
                2 * (UnknownResultTypeList?.Length ?? 1);                       // Result format codes

            buf.WriteByte(Code);
            buf.WriteInt32(messageLength - 1);
            Debug.Assert(Portal == string.Empty);
            buf.WriteByte(0);  // Portal is always empty

            buf.WriteNullTerminatedString(Statement);
            buf.WriteInt16(formatCodeListLength);

            // 0 length implicitly means all-text, 1 means all-binary, >1 means mix-and-match
            if (formatCodeListLength == 1)
            {
                if (buf.WriteSpaceLeft < 2)
                    await buf.Flush(async);
                buf.WriteInt16((short)FormatCode.Binary);
            }
            else if (formatCodeListLength > 1)
            {
                foreach (var p in InputParameters)
                {
                    if (buf.WriteSpaceLeft < 2)
                        await buf.Flush(async);
                    buf.WriteInt16((short)p.FormatCode);
                }
            }

            if (buf.WriteSpaceLeft < 2)
                await buf.Flush(async);

            buf.WriteInt16(InputParameters.Count);

            foreach (var param in InputParameters)
            {
                param.LengthCache?.Rewind();
                await param.WriteWithLength(buf, async);
            }

            if (UnknownResultTypeList != null)
            {
                if (buf.WriteSpaceLeft < 2 + UnknownResultTypeList.Length * 2)
                    await buf.Flush(async);
                buf.WriteInt16(UnknownResultTypeList.Length);
                foreach (var t in UnknownResultTypeList)
                    buf.WriteInt16(t ? 0 : 1);
            }
            else
            {
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                buf.WriteInt16(1);
                buf.WriteInt16(AllResultTypesAreUnknown ? 0 : 1);
            }
        }
#endif
        public override string ToString()
            => $"[Bind(Portal={Portal},Statement={Statement},NumParams={InputParameters.Count}]";
    }
}
