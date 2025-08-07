
using System.Collections;
using System;
using System.Diagnostics;
using System.Linq;
using System.Management.Automation;
using System.Net;

namespace DBAClientX.PowerShell;

[Cmdlet(VerbsCommon.New, "DbaXQuery", DefaultParameterSetName = "Compatibility", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletNewDbaXQuery : PSCmdlet {
    [Parameter(Mandatory = true, Position = 0)]
    [ValidateNotNullOrEmpty]
    public string TableName { get; set; }

    [Parameter(Mandatory = false)]
    public SwitchParameter Compile { get; set; }


    [Parameter(Mandatory = false)]
    public int? Limit { get; set; }

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