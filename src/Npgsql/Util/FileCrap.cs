using System.IO;
using System.Threading;

namespace Npgsql.Util
{
    static class FileCrap
    {
        static object _obj = new object();
        static StreamWriter _writer;

        static FileCrap()
        {
            File.Delete("/tmp/log");
            _writer = new StreamWriter(File.OpenWrite("/tmp/log"));
        }

        internal static void Write(string s)
        {
            lock (_obj)
            {
                _writer.WriteLine($"[{Thread.CurrentThread.ManagedThreadId,2}] {s}");
                _writer.Flush();
            }
        }
    }
}
