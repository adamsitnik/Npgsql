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
    [TypeMapping("integer", NpgsqlDbType.Integer, DbType.Int32, typeof(int))]
    class Int32Handler : NpgsqlSimpleTypeHandler<int>,
        INpgsqlSimpleTypeHandler<byte>, INpgsqlSimpleTypeHandler<short>, INpgsqlSimpleTypeHandler<long>,
        INpgsqlSimpleTypeHandler<float>, INpgsqlSimpleTypeHandler<double>, INpgsqlSimpleTypeHandler<decimal>
    {
        #region Read

        public override int Read(ReadOnlySpan<byte> buf, FieldDescription fieldDescription = null)
            => BinaryPrimitives.ReadInt32BigEndian(buf);

        byte INpgsqlSimpleTypeHandler<byte>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => checked((byte)BinaryPrimitives.ReadInt32BigEndian(buf));

        short INpgsqlSimpleTypeHandler<short>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => checked((short)BinaryPrimitives.ReadInt32BigEndian(buf));

        long INpgsqlSimpleTypeHandler<long>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt32BigEndian(buf);

        float INpgsqlSimpleTypeHandler<float>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt32BigEndian(buf);

        double INpgsqlSimpleTypeHandler<double>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt32BigEndian(buf);

        decimal INpgsqlSimpleTypeHandler<decimal>.Read(ReadOnlySpan<byte> buf, [CanBeNull] FieldDescription fieldDescription)
            => BinaryPrimitives.ReadInt32BigEndian(buf);

        #endregion Read

        #region Write

        public override int ValidateAndGetLength(int value, NpgsqlParameter parameter) => 4;
        public int ValidateAndGetLength(short value, NpgsqlParameter parameter)        => 4;
        public int ValidateAndGetLength(long value, NpgsqlParameter parameter)         => 4;
        public int ValidateAndGetLength(float value, NpgsqlParameter parameter)        => 4;
        public int ValidateAndGetLength(double value, NpgsqlParameter parameter)       => 4;
        public int ValidateAndGetLength(decimal value, NpgsqlParameter parameter)      => 4;
        public int ValidateAndGetLength(byte value, NpgsqlParameter parameter)         => 4;

        public override void Write(int value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, value);
        public void Write(short value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, value);
        public void Write(long value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, checked((int)value));
        public void Write(byte value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, value);
        public void Write(float value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, checked((int)value));
        public void Write(double value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, checked((int)value));
        public void Write(decimal value, Span<byte> buf, NpgsqlParameter parameter)
            => BinaryPrimitives.WriteInt32BigEndian(buf, (int)value);

        #endregion Write
    }
}
