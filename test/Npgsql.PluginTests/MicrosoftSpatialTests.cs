using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Spatial;
using Npgsql.Tests;
using Npgsql.Tests.Types;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.PluginTests
{
    class MicrosoftSpatialTests : TestBase
    {
        public class TestAtt
        {
            public Geometry Geom;
            public string SQL;
        }

        static readonly TestAtt[] Tests =
        {
            new TestAtt { Geom = GeometryPoint.Create(1D, 2500D), SQL = "st_makepoint(1,2500)" },
            new TestAtt {
                Geom = GeometryFactory.LineString(1D, 1D).LineTo(1D, 2500D),
                SQL = "st_makeline(st_makepoint(1,1),st_makepoint(1,2500))"
            },
            /*
                new TestAtt {
                    Geom = new PostgisPolygon(new[] { new[] {
                        new Coordinate2D(1d,1d),
                        new Coordinate2D(2d,2d),
                        new Coordinate2D(3d,3d),
                        new Coordinate2D(1d,1d)
                    }}),
                    SQL = "st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)]))"
                },
                new TestAtt {
                    Geom = new PostgisMultiPoint(new[] { new Coordinate2D(1D, 1D) }),
                    SQL = "st_multi(st_makepoint(1,1))"
                },
                new TestAtt {
                    Geom = new PostgisMultiLineString(new[] {
                        new PostgisLineString(new[] {
                            new Coordinate2D(1D, 1D),
                            new Coordinate2D(1D, 2500D)
                        })
                    }),
                    SQL = "st_multi(st_makeline(st_makepoint(1,1),st_makepoint(1,2500)))"
                },
                new TestAtt {
                    Geom = new PostgisMultiPolygon(new[] {
                        new PostgisPolygon(new[] { new[] {
                            new Coordinate2D(1d,1d),
                            new Coordinate2D(2d,2d),
                            new Coordinate2D(3d,3d),
                            new Coordinate2D(1d,1d)
                        }})
                    }),
                    SQL = "st_multi(st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)])))"
                },
                new TestAtt {
                    Geom = new PostgisGeometryCollection(new PostgisGeometry[] {
                        new PostgisPoint(1,1),
                        new PostgisMultiPolygon(new[] {
                            new PostgisPolygon(new[] { new[] {
                                new Coordinate2D(1d,1d),
                                new Coordinate2D(2d,2d),
                                new Coordinate2D(3d,3d),
                                new Coordinate2D(1d,1d)
                            }})
                        })
                    }),
                    SQL = "st_collect(st_makepoint(1,1),st_multi(st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)]))))"
                },
                new TestAtt {
                    Geom = new PostgisGeometryCollection(new PostgisGeometry[] {
                        new PostgisPoint(1,1),
                        new PostgisGeometryCollection(new PostgisGeometry[] {
                            new PostgisPoint(1,1),
                            new PostgisMultiPolygon(new[] {
                                new PostgisPolygon(new[] { new[] {
                                    new Coordinate2D(1d,1d),
                                    new Coordinate2D(2d,2d),
                                    new Coordinate2D(3d,3d),
                                    new Coordinate2D(1d,1d)
                                }})
                            })
                        })
                    }),
                    SQL = "st_collect(st_makepoint(1,1),st_collect(st_makepoint(1,1),st_multi(st_makepolygon(st_makeline(ARRAY[st_makepoint(1,1),st_makepoint(2,2),st_makepoint(3,3),st_makepoint(1,1)])))))"
                }*/
        };

        [Test,TestCaseSource(nameof(Tests))]
        public void PostgisTestRead(TestAtt att)
        {
            using (var conn = OpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                var a = att;
                cmd.CommandText = "SELECT " + a.SQL;
                var p = cmd.ExecuteScalar();
                Assert.IsTrue(p.Equals(a.Geom));
            }
        }

        [Test, TestCaseSource(nameof(Tests))]
        public void PostgisTestWrite(TestAtt a)
        {
            using (var conn = OpenConnection())
            using (var cmd = conn.CreateCommand())
            {
                // TODO: We currently fail with non-generic NpgsqlParameter because the type
                // actual being written is GeometryPointImplementation :(
                //cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Geometry, a.Geom);
                cmd.Parameters.Add(new NpgsqlParameter<Geometry>
                {
                    ParameterName = "p1",
                    TypedValue = a.Geom
                });
                cmd.CommandText = $"SELECT st_asewkb(:p1) = st_asewkb({a.SQL})";
                Assert.IsTrue((bool)cmd.ExecuteScalar(), "Error on comparison of " + a.Geom);
            }
        }

        #region Support

        protected override NpgsqlConnection OpenConnection(string connectionString = null)
        {
            var conn = new NpgsqlConnection(connectionString ?? ConnectionString);
            conn.Open();
            conn.TypeMapper.UseMicrosoftSpatial();
            return conn;
        }

        [OneTimeSetUp]
        public void SetUp()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT postgis_version()", conn))
            {
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (PostgresException)
                {
                    cmd.CommandText = "SELECT version()";
                    var versionString = (string)cmd.ExecuteScalar();
                    Debug.Assert(versionString != null);
                    var m = Regex.Match(versionString, @"^PostgreSQL ([0-9.]+(\w*)?)");
                    if (!m.Success)
                        throw new Exception("Couldn't parse PostgreSQL version string: " + versionString);
                    var version = m.Groups[1].Value;
                    var prerelease = m.Groups[2].Value;
                    if (!string.IsNullOrWhiteSpace(prerelease))
                        Assert.Ignore($"PostGIS not installed, ignoring because we're on a prerelease version of PostgreSQL ({version})");
                    TestUtil.IgnoreExceptOnBuildServer("PostGIS extension not installed.");
                }
            }
        }

        #endregion Support
    }
}
