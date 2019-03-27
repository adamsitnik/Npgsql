using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.TypeHandling;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Npgsql.Utf8Json
{
    public class JsonbHandlerFactory : NpgsqlTypeHandlerFactory<string>
    {
        protected override NpgsqlTypeHandler<string> Create(NpgsqlConnection conn)
            => new JsonbHandler(conn);
    }

    class JsonbHandler : Npgsql.TypeHandlers.JsonbHandler
    {
        public JsonbHandler(NpgsqlConnection connection) : base(connection) {}

        Span<byte> Foo()
        {
            var b = new byte[10];
            var s = new Span<byte>(b);
            return s;
        }

        protected override ValueTask<T> Read<T>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            if (buf.ReadBytesLeft >= len)
            {
                var span = new ReadOnlySpan<byte>(buf.Buffer, buf.ReadPosition, Math.Min(buf.ReadBytesLeft, len));
                var x = new Utf8JsonReader(span, false, default);
            }
                //? new ValueTask<string>(buf.ReadString(byteLen))
                //: ReadLong();

            throw new NotImplementedException();
        }

        protected override int ValidateAndGetLength<T2>(T2 value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => throw new NotImplementedException();

        protected override Task WriteWithLength<T2>(T2 value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => throw new NotImplementedException();

        protected override int ValidateObjectAndGetLength(object value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => throw new NotImplementedException();

        protected override Task WriteObjectWithLength(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => throw new NotImplementedException();
    }
}
