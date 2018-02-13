using System;
using System.Threading.Tasks;
using Microsoft.Spatial;
using Npgsql.BackendMessages;
using Npgsql.TypeHandling;
using NpgsqlTypes;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Npgsql.Microsoft.Spatial
{
    public class GeometryHandlerFactory : NpgsqlTypeHandlerFactory<Geometry>
    {
        protected override NpgsqlTypeHandler<Geometry> Create(NpgsqlConnection conn)
            => new GeometryHandler();
    }

    class GeometryHandler : NpgsqlTypeHandler<Geometry>
    {
        #region Read

        public override async ValueTask<Geometry> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            await buf.Ensure(9, async);
            var bo = (ByteOrder)buf.ReadByte();
            var id = buf.ReadUInt32(bo);

            if ((id & (uint)EwkbModifiers.HasSRID) != 0)
            {
                // SRID is a PostGIS extension (EWKB) and isn't supported by Microsoft.Spatial
                buf.ReadUInt32(bo);
            }

            switch ((WkbIdentifier)(id & 7))
            {
            case WkbIdentifier.Point:
                await buf.Ensure(16, async);
                return GeometryPoint.Create(buf.ReadDouble(bo), buf.ReadDouble(bo));

            case WkbIdentifier.LineString:
            {
                await buf.Ensure(4, async);
                var numPoints = buf.ReadInt32(bo);
                var lineString = GeometryFactory.LineString();
                for (var i = 0; i < numPoints; i++)
                {
                    await buf.Ensure(16, async);
                    lineString.LineTo(buf.ReadDouble(bo), buf.ReadDouble(bo));
                }

                return lineString;
            }
                    /*
            case WkbIdentifier.Polygon:
            {
                await buf.Ensure(4, async);
                var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                for (var irng = 0; irng < rings.Length; irng++)
                {
                    await buf.Ensure(4, async);
                    rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                    for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                    {
                        await buf.Ensure(16, async);
                        rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                    }
                }
                return new PostgisPolygon(rings);
            }

            case WkbIdentifier.MultiPoint:
            {
                await buf.Ensure(4, async);
                var points = new Coordinate2D[buf.ReadInt32(bo)];
                for (var ipts = 0; ipts < points.Length; ipts++)
                {
                    await buf.Ensure(21, async);
                    await buf.Skip(5, async);
                    points[ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                }
                return new PostgisMultiPoint(points);
            }

            case WkbIdentifier.MultiLineString:
            {
                await buf.Ensure(4, async);
                var rings = new Coordinate2D[buf.ReadInt32(bo)][];

                for (var irng = 0; irng < rings.Length; irng++)
                {
                    await buf.Ensure(9, async);
                    await buf.Skip(5, async);
                    rings[irng] = new Coordinate2D[buf.ReadInt32(bo)];
                    for (var ipts = 0; ipts < rings[irng].Length; ipts++)
                    {
                        await buf.Ensure(16, async);
                        rings[irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                    }
                }
                return new PostgisMultiLineString(rings);
            }

            case WkbIdentifier.MultiPolygon:
            {
                await buf.Ensure(4, async);
                var pols = new Coordinate2D[buf.ReadInt32(bo)][][];

                for (var ipol = 0; ipol < pols.Length; ipol++)
                {
                    await buf.Ensure(9, async);
                    await buf.Skip(5, async);
                    pols[ipol] = new Coordinate2D[buf.ReadInt32(bo)][];
                    for (var irng = 0; irng < pols[ipol].Length; irng++)
                    {
                        await buf.Ensure(4, async);
                        pols[ipol][irng] = new Coordinate2D[buf.ReadInt32(bo)];
                        for (var ipts = 0; ipts < pols[ipol][irng].Length; ipts++)
                        {
                            await buf.Ensure(16, async);
                            pols[ipol][irng][ipts] = new Coordinate2D(buf.ReadDouble(bo), buf.ReadDouble(bo));
                        }
                    }
                }
                return new PostgisMultiPolygon(pols);
            }

            case WkbIdentifier.GeometryCollection:
            {
                await buf.Ensure(4, async);
                var g = new PostgisGeometry[buf.ReadInt32(bo)];

                for (var i = 0; i < g.Length; i++)
                {
                    await buf.Ensure(5, async);
                    var elemBo = (ByteOrder)buf.ReadByte();
                    var elemId = (WkbIdentifier)(buf.ReadUInt32(bo) & 7);

                    g[i] = await DoRead(buf, elemId, elemBo, async);
                }
                return new PostgisGeometryCollection(g);
            }
            */
            default:
                throw new InvalidOperationException("Unknown Postgis identifier.");
            }
        }

        #endregion Read

        #region Write

        public override int ValidateAndGetLength(Geometry value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
        {
            // header =
            //      1 byte for the endianness of the structure
            //    + 4 bytes for the type identifier
            //   (+ 4 bytes for the SRID (EWKB), but we never send SRID)
            var baseLen = 1 + 4;
            switch (value)
            {
            case GeometryPoint _:
                return baseLen + 16;
            case GeometryLineString lineString:
                return baseLen + 4 + lineString.Points.Count * 16;
            default:
                throw new InvalidCastException("Don't know how to write to PostGIS: " + value.GetType().Name);
            }
        }

        public override Task Write(Geometry value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => Write(value, buf, lengthCache, async, true);

        async Task Write(Geometry value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, bool async, bool isRoot)
        {
            if (isRoot)
            {
                // Write header
                if (buf.WriteSpaceLeft < 5)
                    await buf.Flush(async);
                buf.WriteByte(0);  // endianness
            }

            switch (value)
            {
            case GeometryPoint point:
                buf.WriteUInt32((uint)WkbIdentifier.Point);
                if (buf.WriteSpaceLeft < 16)
                    await buf.Flush(async);
                buf.WriteDouble(point.X);
                buf.WriteDouble(point.Y);
                return;

            case GeometryLineString lineString:
                buf.WriteUInt32((uint)WkbIdentifier.LineString);
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async);
                var numPoints = lineString.Points.Count;
                buf.WriteInt32(numPoints);
                foreach (var p in lineString.Points)
                {
                    if (buf.WriteSpaceLeft < 16)
                        await buf.Flush(async);
                    buf.WriteDouble(p.X);
                    buf.WriteDouble(p.Y);
                }
                return;

            default:
                throw new InvalidCastException("Don't know how to write to PostGIS: " + value.GetType().Name);
            }
        }

        #endregion Write
    }

    /// <summary>
    /// Modifiers used by PostGIS to extend the standard WKB
    /// </summary>
    [Flags]
    enum EwkbModifiers : uint
    {
        HasSRID = 0x20000000,
        HasMDim = 0x40000000,
        HasZDim = 0x80000000
    }

    /// <summary>
    /// Represents the identifier of the Well Known Binary representation of a geographical feature specified by the OGC.
    /// http://portal.opengeospatial.org/files/?artifact_id=13227 Chapter 6.3.2.7
    /// </summary>
    enum WkbIdentifier : uint
    {
        Point = 1,
        LineString = 2,
        Polygon = 3,
        MultiPoint = 4,
        MultiLineString = 5,
        MultiPolygon = 6,
        GeometryCollection = 7
    }
}
