using System.Collections.Generic;
using System.Text;
using BenchmarkDotNet.Attributes;
using NpgsqlTypes;

namespace Npgsql.Benchmarks
{
    public class SqlQueryParserBenchmarks
    {
        List<NpgsqlStatement> Queries;
        NpgsqlParameterCollection Params;
        SqlQueryParser Parser = new SqlQueryParser();

        [Params(1, 10, 100, 1000)]
        public int NumStatements { get; set; }

        string Sql;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var sb = new StringBuilder();
            for (var i = 0; i < NumStatements; i++)
                sb.Append("INSERT INTO table (id, name, age, guid, url, notes1, notes2, notes3) VALUES (232, 'Shay Rojansky', @age, '123e4567-e89b-12d3-a456-426655440000', 'http://google.com', @notes1, @notes2, @notes3); ");
            Sql = sb.ToString();

            Queries = new List<NpgsqlStatement>();
            Params = new NpgsqlParameterCollection();
        }

        [Benchmark]
        public void Parse()
        {
            Queries.Clear();
            Params.Clear();
            Parser.ParseRawQuery(Sql, true, Params, Queries);
        }
    }
}
