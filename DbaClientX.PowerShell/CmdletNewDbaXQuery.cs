
using System.Collections;
using System;
using System.Diagnostics;
using System.Linq;
using System.Management.Automation;
using System.Net;

namespace DBAClientX.PowerShell;

/// <summary>Creates a new SQL query using the DbaX query builder.</summary>
/// <para>Constructs a basic SELECT statement against a specified table and optionally compiles it to SQL text.</para>
/// <para>Supports limiting and offsetting rows for paging scenarios.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>The cmdlet does not validate table existence; ensure the target table is correct.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Create a query object.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -TableName 'Users'</code>
/// <para>Returns a query builder object targeting the Users table.</para>
/// </example>
/// <example>
/// <summary>Compile the query to SQL.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -TableName 'Users' -Limit 10 -Compile</code>
/// <para>Outputs the generated SQL statement limited to ten rows.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/sql/t-sql/queries/select-transact-sql">SELECT statement (Transact-SQL)</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsCommon.New, "DbaXQuery", DefaultParameterSetName = "Compatibility", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletNewDbaXQuery : PSCmdlet {
    /// <summary>Name of the table to select from.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    [ValidateNotNullOrEmpty]
    public string TableName { get; set; }

    /// <summary>Compiles the query to a SQL string.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter Compile { get; set; }

    /// <summary>Limits the number of returned rows.</summary>
    [Parameter(Mandatory = false)]
    public int? Limit { get; set; }

    /// <summary>Skips a number of rows before returning results.</summary>
    [Parameter(Mandatory = false)]
    public int? Offset { get; set; }

    private ActionPreference errorAction = ActionPreference.Continue;

    protected override void BeginProcessing() {
        if (MyInvocation.BoundParameters.TryGetValue("ErrorAction", out var value)) {
            if (Enum.TryParse(value.ToString(), true, out ActionPreference actionPreference)) {
                errorAction = actionPreference;
            }
        }
    }

    protected override void ProcessRecord() {
        var query = DBAClientX.QueryBuilder.QueryBuilder.Query().From(TableName);

        if (Limit.HasValue) {
            if (Limit.Value < 0) {
                var message = "Limit must be a non-negative value.";
                WriteWarning(message);
                if (errorAction == ActionPreference.Stop) {
                    ThrowTerminatingError(new ErrorRecord(new PSArgumentException(message), "LimitNegative", ErrorCategory.InvalidArgument, Limit));
                }
            } else {
                query = query.Limit(Limit.Value);
            }
        }

        if (Offset.HasValue) {
            if (Offset.Value < 0) {
                var message = "Offset must be a non-negative value.";
                WriteWarning(message);
                if (errorAction == ActionPreference.Stop) {
                    ThrowTerminatingError(new ErrorRecord(new PSArgumentException(message), "OffsetNegative", ErrorCategory.InvalidArgument, Offset));
                }
            } else {
                query = query.Offset(Offset.Value);
            }
        }

        if (!Compile) {
            WriteObject(query);
        } else {
            var sql = DBAClientX.QueryBuilder.QueryBuilder.Compile(query);
            WriteObject(sql);
        }
    }
}