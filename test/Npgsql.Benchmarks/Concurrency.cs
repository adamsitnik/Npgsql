using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Npgsql.Benchmarks
{
    public class Concurrency
    {
        static readonly string[] InitStatements =
        {
            "DROP TABLE IF EXISTS data",
            "CREATE TABLE data (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)",
            "INSERT INTO data (name) VALUES ('Shay')",
            "INSERT INTO data (name) VALUES ('Dan')",
            "INSERT INTO data (name) VALUES ('Diego')",
            "INSERT INTO data (name) VALUES ('Arthur')"
        };

        string ConnectionString;

        [Params(1)]
        public int NumConnections;

        [Params(20)]
        public int NumThreads;

        [Params(1000)]
        public int IterationsPerThread;

        [GlobalSetup]
        public void GlobalSetup()
        {
            ConnectionString = new NpgsqlConnectionStringBuilder(BenchmarkEnvironment.ConnectionString)
            {
                MaxPoolSize = NumConnections,
                ServerCompatibilityMode = ServerCompatibilityMode.NoTypeLoading,
                NoResetOnClose = true,
                MaxAutoPrepare = 20,
                AutoPrepareMinUsages = 5
            }.ToString();

            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                foreach (var s in InitStatements)
                    using (var cmd = new NpgsqlCommand(s, conn))
                        cmd.ExecuteNonQuery();
            }
        }

        [Benchmark]
        public Task Go()
            => Task.WhenAll(Enumerable
                .Range(0, NumThreads)
                .Select(t => Task.Run(() =>
                {
                    var sum = 0;
                    for (var i = 0; i < IterationsPerThread; i++)
                    {
                        using (var conn = new NpgsqlConnection(ConnectionString))
                        {
                            conn.Open();
                            using (var cmd = new NpgsqlCommand("SELECT name FROM data", conn))
                            using (var reader = cmd.ExecuteReader())
                                while (reader.Read())
                                    unchecked { sum += reader.GetString(0).Length; }
                        }
                    }
                    return sum;
                })));

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            using (var cmd = new NpgsqlCommand("DROP TABLE IF EXISTS data", conn))
            {
                conn.Open();
                cmd.ExecuteNonQuery();
            }
        }
    }
}
