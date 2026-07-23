using System.Runtime.InteropServices;
using System.Security;
using Microsoft.Data.SqlClient;

namespace DBAClientX.PowerShell;

/// <summary>Creates Fabric Warehouse connection options from a caller-acquired secure SQL token.</summary>
/// <example>
/// <summary>Create options from an Az.Accounts database token.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$token = Get-AzAccessToken -ResourceUrl 'https://database.windows.net' -AsSecureString; $options = New-DbaXFabricWarehouseConnectionOptions -AccessToken $token.Token -ExpiresOn $token.ExpiresOn</code>
/// <para>The reusable callback preserves SqlClient pooling while the fixed token remains valid.</para>
/// </example>
[Cmdlet(VerbsCommon.New, "DbaXFabricWarehouseConnectionOptions")]
[OutputType(typeof(SqlServerConnectionOptions))]
[CmdletBinding()]
public sealed class CmdletNewDbaXFabricWarehouseConnectionOptions : PSCmdlet
{
    /// <summary>Caller-acquired Microsoft Entra token for the Azure SQL resource.</summary>
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
            var expiresOn = ExpiresOn;
            Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>> callback =
                (_, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (expiresOn <= DateTimeOffset.UtcNow.AddMinutes(1))
                    {
                        throw new InvalidOperationException(
                            "The fixed Fabric Warehouse access token is expired or too close to expiry.");
                    }

                    return Task.FromResult(new SqlAuthenticationToken(token, expiresOn));
                };

            WriteObject(new SqlServerConnectionOptions
            {
                CompatibilityProfile = SqlServerCompatibilityProfile.FabricWarehouse,
                AccessTokenCallback = callback
            });
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
