using System.IO;

namespace Npgsql.Util
{
    public static class Tov
    {
        static object _lock = new object();
        static StreamWriter _writer = new StreamWriter("/tmp/crap");

        public static void Log(string msg)
        {
            lock (_lock)
            {
                _writer.WriteLine(msg);
                _writer.Flush();
            }
        }
    }
}
