#if NET461 || NETSTANDARD2_0 || NETSTANDARD2_1
#pragma warning disable 1591
// ReSharper disable once CheckNamespace
namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    public sealed class MaybeNullWhenAttribute : Attribute
    {
        public bool ReturnValue { get; }

        public MaybeNullWhenAttribute(bool returnValue)
            => ReturnValue = returnValue;
    }
}
#endif
