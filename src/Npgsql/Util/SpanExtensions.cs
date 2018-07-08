using System;
using System.Diagnostics;
using System.Text;

namespace Npgsql.Util
{
    static class SpanExtensions
    {
        internal static int WriteNullTerminatedString(this Span<byte> span, Encoding encoding, string s)
        {
            Debug.Assert(encoding.GetByteCount(s)+1 <= span.Length);

            var len = span.WriteString(encoding, s);
            span[len] = 0;
            return len + 1;
        }

        internal static unsafe int WriteString(this Span<byte> span, Encoding encoding, string s)
        {
            Debug.Assert(encoding.GetByteCount(s) <= span.Length);

#if NETCOREAPP2_1
            return encoding.GetBytes(s, span);
#else
            fixed (byte* p = &span.GetPinnableReference())
            fixed (char* c = s)
            {
                return encoding.GetBytes(c, s.Length, p, span.Length);
            }
#endif
        }
    }
}
