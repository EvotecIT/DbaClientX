#if NET472
namespace System.Diagnostics.CodeAnalysis;

[System.Flags]
internal enum DynamicallyAccessedMemberTypes
{
    None = 0,
    PublicParameterlessConstructor = 0x0001,
    PublicConstructors = 0x0002 | PublicParameterlessConstructor,
    NonPublicConstructors = 0x0004,
    PublicMethods = 0x0008,
    NonPublicMethods = 0x0010,
    PublicFields = 0x0020,
    NonPublicFields = 0x0040,
    PublicNestedTypes = 0x0080,
    NonPublicNestedTypes = 0x0100,
    PublicProperties = 0x0200,
    NonPublicProperties = 0x0400,
    PublicEvents = 0x0800,
    NonPublicEvents = 0x1000,
    Interfaces = 0x2000,
    All = ~None
}

[System.AttributeUsage(
    System.AttributeTargets.Field |
    System.AttributeTargets.ReturnValue |
    System.AttributeTargets.GenericParameter |
    System.AttributeTargets.Parameter |
    System.AttributeTargets.Property |
    System.AttributeTargets.Method |
    System.AttributeTargets.Class |
    System.AttributeTargets.Struct)]
internal sealed class DynamicallyAccessedMembersAttribute : System.Attribute
{
    public DynamicallyAccessedMembersAttribute(DynamicallyAccessedMemberTypes memberTypes)
        => MemberTypes = memberTypes;

    public DynamicallyAccessedMemberTypes MemberTypes { get; }
}

[System.AttributeUsage(System.AttributeTargets.Method | System.AttributeTargets.Constructor | System.AttributeTargets.Class | System.AttributeTargets.Struct)]
internal sealed class RequiresUnreferencedCodeAttribute : System.Attribute
{
    public RequiresUnreferencedCodeAttribute(string message)
        => Message = message;

    public string Message { get; }

    public string? Url { get; set; }
}

[System.AttributeUsage(System.AttributeTargets.All, AllowMultiple = true, Inherited = false)]
internal sealed class UnconditionalSuppressMessageAttribute : System.Attribute
{
    public UnconditionalSuppressMessageAttribute(string category, string checkId)
    {
        Category = category;
        CheckId = checkId;
    }

    public string Category { get; }

    public string CheckId { get; }

    public string? Scope { get; set; }

    public string? Target { get; set; }

    public string? MessageId { get; set; }

    public string? Justification { get; set; }
}
#endif
