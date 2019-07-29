#if NET461 || NETSTANDARD2_0 || NETSTANDARD2_1
#pragma warning disable 1591
// ReSharper disable once CheckNamespace
namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    public sealed class NotNullWhenAttribute : Attribute
    {
        public bool ReturnValue { get; }

        public NotNullWhenAttribute(bool returnValue)
            => ReturnValue = returnValue;
    }
}
#endif
