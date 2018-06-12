using System;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Npgsql.Benchmarks
{
    public class ReadString
    {
        const int StringSize = 256;
        byte[] Buffer = new byte[StringSize];
        readonly Encoding Encoding = Encoding.UTF8;

        public ReadString()
        {
            var data = new string('x', StringSize);
            Encoding.GetBytes(data, 0, StringSize, Buffer, 0);
        }

        [Benchmark(Baseline = true)]
        public string Array() => Encoding.GetString(Buffer, 0, StringSize);

        [Benchmark]
        public unsafe string SpanLegacy()
        {
            var span = new Span<byte>(Buffer);
            fixed (byte* bytes = &span.GetPinnableReference())
            {
                return Encoding.GetString(bytes, StringSize);
            }
        }

#if NETCOREAPP2_1
        [Benchmark]
        public string Span21()
        {
            var span = new Span<byte>(Buffer);
            return Encoding.GetString(span);
        }
#endif
    }
}
