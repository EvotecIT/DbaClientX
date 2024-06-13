
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
    public string TableName { get; set; }

    [Parameter(Mandatory = false)]
    public SwitchParameter Compile { get; set; }

    [Parameter(Mandatory = false)]
    public SwitchParameter DontUseLegacyPagination { get; set; }

    private ActionPreference errorAction;

    protected override void BeginProcessing() {
        // Get the error action preference as user requested
        // It first sets the error action to the default error action preference
        // If the user has specified the error action, it will set the error action to the user specified error action
        errorAction = (ActionPreference)this.SessionState.PSVariable.GetValue("ErrorActionPreference");
        if (this.MyInvocation.BoundParameters.ContainsKey("ErrorAction")) {
            string errorActionString = this.MyInvocation.BoundParameters["ErrorAction"].ToString();
            if (Enum.TryParse(errorActionString, true, out ActionPreference actionPreference)) {
                errorAction = actionPreference;
            }
        }
    }

    protected override void ProcessRecord() {
        var compiler = new SqlKata.Compilers.SqlServerCompiler() {
            UseLegacyPagination = !DontUseLegacyPagination
        };
        var query = new SqlKata.Query(TableName);
        if (!Compile) {
            WriteObject(query);
        } else {
            WriteObject(compiler.Compile(query).ToString());
        }
    }
}