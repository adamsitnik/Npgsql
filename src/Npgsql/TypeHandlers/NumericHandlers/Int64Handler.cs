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
using Npgsql.BackendMessages;
using NpgsqlTypes;
using System.Data;
using System.Diagnostics;
using JetBrains.Annotations;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandling;
using Npgsql.TypeMapping;

namespace Npgsql.TypeHandlers.NumericHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/datatype-numeric.html
    /// </remarks>
    [TypeMapping("bigint", NpgsqlDbType.Bigint, DbType.Int64, typeof(long))]
    class Int64Handler : NpgsqlSimpleTypeHandler<long>,
        INpgsqlSimpleTypeHandler<byte>, INpgsqlSimpleTypeHandler<short>, INpgsqlSimpleTypeHandler<int>,
        INpgsqlSimpleTypeHandler<float>, INpgsqlSimpleTypeHandler<double>, INpgsqlSimpleTypeHandler<decimal>
    {
        #region Read

        public override long Read(ReadOnlySpan<byte> buf, FieldDescription fieldDescription = null)
            => BinaryPrimitives.ReadInt64BigEndian(buf);

        byte INpgsqlSimpleTypeHandler<byte>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => checked((byte)BinaryPrimitives.ReadInt64BigEndian(buf));

        short INpgsqlSimpleTypeHandler<short>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => checked((short)BinaryPrimitives.ReadInt64BigEndian(buf));

        int INpgsqlSimpleTypeHandler<int>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => checked((int)BinaryPrimitives.ReadInt64BigEndian(buf));

        float INpgsqlSimpleTypeHandler<float>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt64BigEndian(buf);

        double INpgsqlSimpleTypeHandler<double>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt64BigEndian(buf);

        decimal INpgsqlSimpleTypeHandler<decimal>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt64BigEndian(buf);

        #endregion Read

        #region Write

        public override int ValidateAndGetLength(long value, NpgsqlParameter parameter) => 8;
        public int ValidateAndGetLength(short value, NpgsqlParameter parameter)         => 8;
        public int ValidateAndGetLength(int value, NpgsqlParameter parameter)           => 8;
        public int ValidateAndGetLength(float value, NpgsqlParameter parameter)         => 8;
        public int ValidateAndGetLength(double value, NpgsqlParameter parameter)        => 8;
        public int ValidateAndGetLength(decimal value, NpgsqlParameter parameter)       => 8;
        public int ValidateAndGetLength(byte value, NpgsqlParameter parameter)          => 8;

        public override void Write(long value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, value);
        public void Write(short value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, value);
        public void Write(int value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, value);
        public void Write(byte value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, value);
        public void Write(float value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, checked((long)value));
        public void Write(double value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, checked((long)value));
        public void Write(decimal value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt64BigEndian(buf, (long)value);

        #endregion Write
    }
}
