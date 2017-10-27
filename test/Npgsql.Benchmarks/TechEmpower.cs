using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Npgsql.Benchmarks
{
    [Config("columns=OperationPerSecond")]
    [MemoryDiagnoser]
    public class TechEmpower
    {
        public long Sum;

        [Benchmark]
        public void InstantiatePrepareExecuteParse()
        {
            using (var conn = BenchmarkEnvironment.OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT 8, 'hello'", conn))
            {
                cmd.Prepare();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        unchecked
                        {
                            Sum += reader.GetInt32(0);
                            Sum += reader.GetString(1).Length;
                        }
                    }
                }
            }
        }
    }
}
