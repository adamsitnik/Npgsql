using System;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Npgsql.Benchmarks
{
    public class WriteString
    {
        const int StringSize = 256;
        string Data = new string('x', StringSize);
        byte[] Buffer = new byte[1024];

        readonly Encoding Encoding = Encoding.UTF8;

        [Benchmark(Baseline = true)]
        public void Array() => Encoding.GetBytes(Data, 0, 256, Buffer, 0);

        [Benchmark]
        public unsafe void SpanLegacy()
        {
            var span = new Span<byte>(Buffer);
            fixed (char* chars = Data)
            fixed (byte* bytes = &span.GetPinnableReference())
            {
                Encoding.GetBytes(chars, StringSize, bytes, 1024);
            }
        }

#if NETCOREAPP2_1
        [Benchmark]
        public void Span21()
        {
            var span = new Span<byte>(Buffer);
            Encoding.GetBytes(Data, span);
        }
#endif
    }
}
