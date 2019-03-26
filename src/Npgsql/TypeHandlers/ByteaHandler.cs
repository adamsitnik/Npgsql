using System;
using System.Data;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.TypeHandling;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.TypeHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/datatype-binary.html
    /// </remarks>
    [TypeMapping("bytea", NpgsqlDbType.Bytea, DbType.Binary,
        new[] { typeof(byte[]), typeof(ArraySegment<byte>), typeof(Memory<byte>) })]
    public class ByteaHandler : NpgsqlTypeHandler<byte[]>,
        INpgsqlTypeHandler<ArraySegment<byte>>, INpgsqlTypeHandler<Memory<byte>>
    {
        /// <inheritdoc />
        public override async ValueTask<byte[]> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            var bytes = new byte[len];
            var pos = 0;
            while (true)
            {
                var toRead = Math.Min(len - pos, buf.ReadBytesLeft);
                buf.ReadBytes(bytes, pos, toRead);
                pos += toRead;
                if (pos == len)
                    break;
                await buf.ReadMore(async);
            }
            return bytes;
        }

        /// <inheritdoc />
        ValueTask<Memory<byte>> INpgsqlTypeHandler<Memory<byte>>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => throw new NotImplementedException();

        ValueTask<ArraySegment<byte>> INpgsqlTypeHandler<ArraySegment<byte>>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
        {
            buf.Skip(len);
            throw new NpgsqlSafeReadException(new NotSupportedException("Only writing ArraySegment<byte> to PostgreSQL bytea is supported, no reading."));
        }

        #region Write

        /// <inheritdoc />
        public override int ValidateAndGetLength(byte[] value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => parameter == null || parameter.Size <= 0 || parameter.Size >= value.Length
                    ? value.Length
                    : parameter.Size;

        /// <inheritdoc />
        public int ValidateAndGetLength(ArraySegment<byte> value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => parameter == null || parameter.Size <= 0 || parameter.Size >= value.Count
                ? value.Count
                : parameter.Size;

        /// <inheritdoc />
        public int ValidateAndGetLength(Memory<byte> value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => parameter == null || parameter.Size <= 0 || parameter.Size >= value.Length
                ? value.Length
                : parameter.Size;

        /// <inheritdoc />
        public override async Task Write(byte[] value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, [CanBeNull] NpgsqlParameter parameter, bool async)
        {
            var len = parameter == null || parameter.Size <= 0 || parameter.Size >= value.Length
                ? value.Length
                : parameter.Size;

            // The entire array fits in our buffer, copy it into the buffer as usual.
            if (len <= buf.WriteSpaceLeft)
            {
                buf.WriteBytes(value);
                return;
            }

            // The segment is larger than our buffer. Flush whatever is currently in the buffer and
            // write the array directly to the socket.
            await buf.Flush(async);
#if NET461
            buf.DirectWrite(value, 0, len);
#else
            buf.DirectWrite(value);
#endif
        }

        /// <inheritdoc />
        public async Task Write(ArraySegment<byte> value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, [CanBeNull] NpgsqlParameter parameter, bool async)
        {
            if (!(parameter == null || parameter.Size <= 0 || parameter.Size >= value.Count))
                value = new ArraySegment<byte>(value.Array, value.Offset, Math.Min(parameter.Size, value.Count));

            // The entire segment fits in our buffer, copy it as usual.
            if (value.Count <= buf.WriteSpaceLeft)
            {
                buf.WriteBytes(value.AsSpan());
                return;
            }

            // The segment is larger than our buffer. Flush whatever is currently in the buffer and
            // write the array directly to the socket.
            await buf.Flush(async);

#if NET461
            buf.DirectWrite(value.Array, value.Offset, value.Count);
#else
            buf.DirectWrite(value.AsSpan());
#endif
        }

        /// <inheritdoc />
        public async Task Write(Memory<byte> value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
        {
            var len = parameter == null || parameter.Size <= 0 || parameter.Size >= value.Length
                ? value.Length
                : parameter.Size;

            // The entire array fits in our buffer, copy it into the buffer as usual.
            if (len <= buf.WriteSpaceLeft)
            {
                buf.WriteBytes(value.Span);
                return;
            }

            // The segment is larger than our buffer. Flush whatever is currently in the buffer and
            // write the array directly to the socket.
            await buf.Flush(async);

#if NET461
            buf.DirectWrite(value, 0, len);
#else
            buf.DirectWrite(value.Span);
#endif
        }

        #endregion
    }
}
