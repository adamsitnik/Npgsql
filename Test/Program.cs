using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Test
{
    class Program
    {
        const string ConnString = "Host=localhost;Database=npgsql_tests;Username=npgsql_tests;Password=npgsql_tests";
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var random = new Random();
            var longConn = new NpgsqlConnection(ConnString);
            longConn.Open();

            while (true)
            {
                await Task.WhenAll(Enumerable
                    .Range(0, random.Next(1, 5))
                    .Select(i => Task.Run(() =>
                    {
                        using (var conn = new NpgsqlConnection(ConnString))
                        {
                            conn.Open();
                            var rand = random.Next(0, 10);
                            var sql = rand == 9 ? "BAD SQL" : $"SELECT pg_sleep({1/random.Next(1, 10)})";
                            try
                            {
                                using (var cmd = new NpgsqlCommand(sql, conn))
                                    cmd.ExecuteScalar();
                            } catch {}
                        }
                    })));
            }
        }
    }
}
