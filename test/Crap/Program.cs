using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Crap
{
    class Program
    {
        const int Iterations = 100000;

        static void Main(string[] args)
        {
            using (var conn = new NpgsqlConnection("Host=localhost;Username=npgsql_tests;Password=npgsql_tests"))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand("SELECT 1, 'foo'", conn))
                {
                    cmd.Prepare();
                    long sum = 0;
                    for (var i = 0; i < Iterations; i++)
                    {
                        using (var reader = cmd.ExecuteReader())
                        {
                            reader.Read();
                            unchecked
                            {
                                sum += reader.GetInt32(0);
                                sum += reader.GetString(1).Length;
                            }
                        }
                    }
                }
            }
        }
    }
}
