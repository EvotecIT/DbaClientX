using System.Runtime.InteropServices;
using System.Security;
using FabricClientX;

namespace DBAClientX.PowerShell;

/// <summary>Creates a short-lived FabricClientX token provider from a caller-acquired secure token.</summary>
/// <example>
/// <summary>Create a provider from an Az.Accounts token result.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$azToken = Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com' -AsSecureString; $provider = New-DbaXFabricTokenProvider -AccessToken $azToken.Token -ExpiresOn $azToken.ExpiresOn</code>
/// <para>The returned provider can be reused by Fabric discovery cmdlets until the token nears expiry.</para>
/// </example>
[Cmdlet(VerbsCommon.New, "DbaXFabricTokenProvider")]
[OutputType(typeof(IFabricTokenProvider))]
[CmdletBinding()]
public sealed class CmdletNewDbaXFabricTokenProvider : PSCmdlet
{
    /// <summary>Caller-acquired Microsoft Entra access token.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    [ValidateNotNull]
    public SecureString AccessToken { get; set; } = null!;

    /// <summary>Token expiry reported by the identity provider.</summary>
    [Parameter(Mandatory = true, Position = 1)]
    public DateTimeOffset ExpiresOn { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        var pointer = IntPtr.Zero;
        try
        {
            pointer = Marshal.SecureStringToBSTR(AccessToken);
            var token = Marshal.PtrToStringBSTR(pointer)
                ?? throw new InvalidOperationException("The secure access token could not be read.");
            WriteObject(new FixedFabricTokenProvider(token, ExpiresOn));
        }
        finally
        {
            if (pointer != IntPtr.Zero)
            {
                Marshal.ZeroFreeBSTR(pointer);
            }
        }
    }
}
