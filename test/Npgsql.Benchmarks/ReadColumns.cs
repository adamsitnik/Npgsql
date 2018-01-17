using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Npgsql.Benchmarks
{
    [Config("columns=OperationPerSecond")]
    public class ReadColumns
    {
        NpgsqlConnection _conn;
        NpgsqlCommand _cmd;

        [Params(1, 10, 100, 1000)]
        public int NumColumns { get; set; } = 100;

        static readonly string[] Queries;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _conn = BenchmarkEnvironment.OpenConnection();
            _cmd = new NpgsqlCommand(Queries[NumColumns], _conn);
            _cmd.Prepare();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _cmd.Dispose();
            _conn.Dispose();
        }

        [Benchmark]
        public async Task<int> IntColumnAsync()
        {
            unchecked
            {
                var x = 0;
                using (var reader = await _cmd.ExecuteReaderAsync())
                {
                    await reader.ReadAsync();
                    for (var i = 0; i < NumColumns; i++)
                        x += reader.GetInt32(i);
                }
                return x;
            }
        }

        [Benchmark]
        public async Task<int> IntColumn()
        {
            unchecked
            {
                var x = 0;
                using (var reader = _cmd.ExecuteReader())
                {
                    await reader.ReadAsync();
                    for (var i = 0; i < NumColumns; i++)
                        x += reader.GetInt32(i);
                }
                return x;
            }
        }

        static ReadColumns()
        {
            Queries = new string[NumColumnsValues.Max() + 1];
            Queries[0] = "SELECT 1 WHERE 1=0";

            foreach (var numColumns in NumColumnsValues.Where(i => i != 0))
                Queries[numColumns] = GenerateQuery(numColumns);
        }

        static string GenerateQuery(int numColumns)
        {
            var sb = new StringBuilder()
                .Append("SELECT ")
                .Append(string.Join(",", Enumerable.Range(0, numColumns)))
                .Append(";");
            return sb.ToString();
        }

        static readonly int[] NumColumnsValues = typeof(ReadColumns)
            .GetProperty(nameof(NumColumns))
            .GetCustomAttribute<ParamsAttribute>()
            .Values
            .Cast<int>()
            .ToArray();
    }
}
