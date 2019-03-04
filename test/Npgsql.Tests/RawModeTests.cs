using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Tests
{
    public class RawModeTests : TestBase
    {
        [Test]
        public void Basic()
        {
            using (var conn = OpenConnection())
            {
                // Single raw command
                using (var cmd = new NpgsqlCommand("SELECT $1", conn))
                {
                    cmd.Parameters.AddWithValue("hello");
                    Assert.That(cmd.ExecuteScalar(), Is.EqualTo("hello"));
                }

                // Set of raw commands
                using (var cmdSet = new NpgsqlCommandSet
                {
                    Connection = conn,
                    Commands =
                    {
                        new NpgsqlCommand("SELECT $1")
                        {
                            Parameters = { new NpgsqlParameter { Value = "first" } }
                        },
                        new NpgsqlCommand("SELECT $1, $2")
                        {
                            Parameters = {
                                new NpgsqlParameter { Value = "second" },
                                new NpgsqlParameter { Value = "third" }
                            }
                        },
                    }
                })
                using (var reader = cmdSet.ExecuteReader())
                {
                    Assert.True(reader.Read());
                    Assert.That(reader.GetString(0), Is.EqualTo("first"));
                    Assert.True(reader.NextResult());
                    Assert.True(reader.Read());
                    Assert.That(reader.GetString(0), Is.EqualTo("second"));
                    Assert.That(reader.GetString(1), Is.EqualTo("third"));
                }
            }
        }

        protected override NpgsqlConnection OpenConnection(string connectionString = null)
            => base.OpenConnection(connectionString ?? DefaultConnectionString);

        static readonly string DefaultConnectionString;

        static RawModeTests()
        {
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString);
            csb.RawSqlMode = true;
            DefaultConnectionString = csb.ToString();
        }
    }
}
