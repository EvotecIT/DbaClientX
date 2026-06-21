
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Management.Automation;
using DBAClientX.QueryBuilder;
using DbaQueryBuilder = DBAClientX.QueryBuilder.QueryBuilder;

namespace DBAClientX.PowerShell;

/// <summary>
/// Describes which query-builder operation <c>New-DbaXQuery</c> should create.
/// </summary>
public enum DbaXQueryAction {
    /// <summary>Create a SELECT query.</summary>
    Select,

    /// <summary>Create an INSERT query.</summary>
    Insert,

    /// <summary>Create an UPDATE query.</summary>
    Update,

    /// <summary>Create a DELETE query.</summary>
    Delete,

    /// <summary>Create a provider-aware insert-or-update query.</summary>
    Upsert
}

/// <summary>Creates SQL query-builder objects using the DbaClientX core query builder.</summary>
/// <para>Builds SELECT, INSERT, UPDATE, DELETE, and UPSERT statements without duplicating SQL-generation logic in PowerShell.</para>
/// <para>Use <c>-Compile</c> for literal SQL output or <c>-CompileWithParameters</c> to return SQL plus an ordered parameter map.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>The cmdlet does not connect to the database or validate table existence; it only builds query objects or SQL text.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Create a SELECT query object.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -TableName 'dbo.Users' -Columns Id,DisplayName</code>
/// <para>Returns a core query object targeting selected columns from dbo.Users.</para>
/// </example>
/// <example>
/// <summary>Compile a paged SELECT query.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -TableName 'dbo.Users' -Columns Id,DisplayName -Where @{ IsActive = $true } -OrderBy DisplayName -Limit 10 -Offset 20 -Compile</code>
/// <para>Outputs a SQL Server SELECT statement with a WHERE clause and OFFSET/FETCH pagination.</para>
/// </example>
/// <example>
/// <summary>Compile an INSERT statement.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -Action Insert -TableName 'dbo.Users' -Values ([ordered]@{ Id = 42; DisplayName = 'Ada' }) -Compile</code>
/// <para>Outputs an INSERT statement using the DbaClientX core query compiler.</para>
/// </example>
/// <example>
/// <summary>Compile an UPDATE statement.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -Action Update -TableName 'dbo.Users' -Set @{ DisplayName = 'Ada Lovelace' } -Where @{ Id = 42 } -Compile</code>
/// <para>Outputs an UPDATE statement with values supplied by PowerShell hashtables.</para>
/// </example>
/// <example>
/// <summary>Compile a DELETE statement.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -Action Delete -TableName 'dbo.Users' -Where @{ Id = 42 } -Compile</code>
/// <para>Outputs a DELETE statement scoped by the provided WHERE values.</para>
/// </example>
/// <example>
/// <summary>Compile a provider-specific UPSERT statement.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -Action Upsert -Dialect PostgreSql -TableName 'public.users' -Values ([ordered]@{ id = 42; display_name = 'Ada'; email = 'ada@example.test' }) -ConflictColumns id -UpsertUpdateOnly display_name,email -Compile</code>
/// <para>Outputs a PostgreSQL INSERT ... ON CONFLICT statement. Use another <c>-Dialect</c> value for SQL Server, MySQL, SQLite, or Oracle compiler behavior.</para>
/// </example>
/// <example>
/// <summary>Compile SQL with ordered parameters.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXQuery -Action Update -Dialect PostgreSql -TableName 'public.users' -Set @{ display_name = 'Ada' } -Where @{ id = 42 } -CompileWithParameters</code>
/// <para>Returns an object with <c>Sql</c>, <c>Parameters</c>, and <c>ParameterValues</c> properties for callers that want to execute parameterized SQL later.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/sql/t-sql/queries/select-transact-sql">SELECT statement (Transact-SQL)</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsCommon.New, "DbaXQuery", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletNewDbaXQuery : PSCmdlet {
    /// <summary>The query-builder operation to create.</summary>
    [Parameter(Mandatory = false)]
    public DbaXQueryAction Action { get; set; } = DbaXQueryAction.Select;

    /// <summary>Name of the table targeted by the query.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    [ValidateNotNullOrEmpty]
    public string TableName { get; set; } = string.Empty;

    /// <summary>Columns to include in a SELECT statement. When omitted, the core builder emits <c>*</c>.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public string[]? Columns { get; set; }

    /// <summary>Column/value pairs for INSERT or UPSERT statements.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public IDictionary? Values { get; set; }

    /// <summary>Column/value pairs for UPDATE SET clauses.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public IDictionary? Set { get; set; }

    /// <summary>Column/value pairs added as equality predicates to the WHERE clause.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public IDictionary? Where { get; set; }

    /// <summary>Columns used to detect UPSERT conflicts.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public string[]? ConflictColumns { get; set; }

    /// <summary>Columns updated during an UPSERT conflict. When omitted, the core builder updates all non-conflict insert columns.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public string[]? UpsertUpdateOnly { get; set; }

    /// <summary>Columns to add to ORDER BY in ascending order.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public string[]? OrderBy { get; set; }

    /// <summary>Columns to add to ORDER BY in descending order.</summary>
    [Parameter(Mandatory = false)]
    [ValidateNotNullOrEmpty]
    public string[]? OrderByDescending { get; set; }

    /// <summary>Compiles the query to a SQL string.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter Compile { get; set; }

    /// <summary>Compiles the query to SQL text and returns ordered parameter values.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter CompileWithParameters { get; set; }

    /// <summary>SQL dialect used when compiling the query.</summary>
    [Parameter(Mandatory = false)]
    public SqlDialect Dialect { get; set; } = SqlDialect.SqlServer;

    /// <summary>Limits the number of returned rows.</summary>
    [Parameter(Mandatory = false)]
    public int? Limit { get; set; }

    /// <summary>Skips a number of rows before returning results.</summary>
    [Parameter(Mandatory = false)]
    public int? Offset { get; set; }

    private ActionPreference errorAction = ActionPreference.Continue;

    /// <summary>
    /// Initializes cmdlet state before pipeline execution begins.
    /// </summary>
    protected override void BeginProcessing() {
        if (MyInvocation.BoundParameters.TryGetValue("ErrorAction", out var value)) {
            if (Enum.TryParse(value.ToString(), true, out ActionPreference actionPreference)) {
                errorAction = actionPreference;
            }
        }
    }

    /// <summary>
    /// Processes input and performs the cmdlet's primary work.
    /// </summary>
    protected override void ProcessRecord() {
        var query = BuildQuery();

        if (CompileWithParameters) {
            var compiled = DbaQueryBuilder.CompileWithParameters(query, Dialect);
            WriteObject(new PSObject(new {
                compiled.Sql,
                Parameters = BuildParameterMap(compiled.Parameters),
                ParameterValues = compiled.Parameters.ToArray()
            }));
        } else if (Compile) {
            WriteObject(DbaQueryBuilder.Compile(query, Dialect));
        } else {
            WriteObject(query);
        }
    }

    private Query BuildQuery() {
        ValidateUnsupportedOptions();
        var query = DbaQueryBuilder.Query();

        switch (Action) {
            case DbaXQueryAction.Select:
                query.From(TableName);
                if (Columns is { Length: > 0 }) {
                    query.Select(Columns);
                }
                break;
            case DbaXQueryAction.Insert:
                ApplyInsert(query);
                break;
            case DbaXQueryAction.Update:
                ApplyUpdate(query);
                break;
            case DbaXQueryAction.Delete:
                query.DeleteFrom(TableName);
                break;
            case DbaXQueryAction.Upsert:
                ApplyUpsert(query);
                break;
            default:
                throw new PSArgumentException($"Unsupported query action '{Action}'.", nameof(Action));
        }

        ApplyWhere(query);
        ApplyOrdering(query);
        ApplyPaging(query);
        return query;
    }

    private void ApplyInsert(Query query) {
        var pairs = GetPairs(Values, nameof(Values), allowNullValues: true);
        query.InsertInto(TableName, pairs.Select(pair => pair.Column).ToArray())
            .Values(ToValueArray(pairs));
    }

    private void ApplyUpdate(Query query) {
        var pairs = GetPairs(Set, nameof(Set), allowNullValues: false);
        query.Update(TableName);
        foreach (var pair in pairs) {
            query.Set(pair.Column, pair.Value!);
        }
    }

    private void ApplyUpsert(Query query) {
        var pairs = GetPairs(Values, nameof(Values), allowNullValues: false);
        if (ConflictColumns is not { Length: > 0 }) {
            throw new PSArgumentException("ConflictColumns must be provided for upsert queries.", nameof(ConflictColumns));
        }

        query.InsertOrUpdate(TableName, pairs.Select(pair => (pair.Column, Value: pair.Value!)), ConflictColumns);
        if (UpsertUpdateOnly is { Length: > 0 }) {
            query.UpsertUpdateOnly(UpsertUpdateOnly);
        }
    }

    private void ApplyWhere(Query query) {
        if (Where == null) {
            return;
        }

        foreach (var pair in GetPairs(Where, nameof(Where), allowNullValues: false)) {
            query.Where(pair.Column, pair.Value!);
        }
    }

    private void ApplyOrdering(Query query) {
        if (OrderBy is { Length: > 0 }) {
            query.OrderBy(OrderBy);
        }

        if (OrderByDescending is { Length: > 0 }) {
            query.OrderByDescending(OrderByDescending);
        }
    }

    private void ApplyPaging(Query query) {
        if (Limit.HasValue) {
            if (Limit.Value < 0) {
                HandleNonNegativeValidation("Limit", Limit.Value);
            } else {
                query.Limit(Limit.Value);
            }
        }

        if (Offset.HasValue) {
            if (Offset.Value < 0) {
                HandleNonNegativeValidation("Offset", Offset.Value);
            } else {
                query.Offset(Offset.Value);
            }
        }
    }

    private void HandleNonNegativeValidation(string parameterName, int value) {
        var message = $"{parameterName} must be a non-negative value.";
        WriteWarning(message);
        if (errorAction == ActionPreference.Stop) {
            ThrowTerminatingError(new ErrorRecord(new PSArgumentException(message), $"{parameterName}Negative", ErrorCategory.InvalidArgument, value));
        }
    }

    private void ValidateUnsupportedOptions() {
        if (Action == DbaXQueryAction.Select) {
            return;
        }

        if (Limit.HasValue || Offset.HasValue || OrderBy is { Length: > 0 } || OrderByDescending is { Length: > 0 }) {
            throw new PSArgumentException("Limit, Offset, OrderBy, and OrderByDescending are only supported for Select queries.");
        }

        if ((Action == DbaXQueryAction.Insert || Action == DbaXQueryAction.Upsert) && Where != null) {
            throw new PSArgumentException("Where is only supported for Select, Update, and Delete queries.");
        }
    }

    private static OrderedDictionary BuildParameterMap(IReadOnlyList<object> parameters) {
        var map = new OrderedDictionary();
        for (var i = 0; i < parameters.Count; i++) {
            map.Add("@p" + i, parameters[i]);
        }

        return map;
    }

    private static object[] ToValueArray(IReadOnlyList<(string Column, object? Value)> pairs) {
        var values = new object[pairs.Count];
        for (var i = 0; i < pairs.Count; i++) {
            values[i] = pairs[i].Value!;
        }

        return values;
    }

    private static IReadOnlyList<(string Column, object? Value)> GetPairs(IDictionary? dictionary, string parameterName, bool allowNullValues) {
        if (dictionary == null || dictionary.Count == 0) {
            throw new PSArgumentException($"{parameterName} must contain at least one column/value pair.", parameterName);
        }

        var pairs = new List<(string Column, object? Value)>(dictionary.Count);
        foreach (DictionaryEntry entry in dictionary) {
            var column = entry.Key?.ToString();
            if (string.IsNullOrWhiteSpace(column)) {
                throw new PSArgumentException($"{parameterName} contains an empty column name.", parameterName);
            }

            if (!allowNullValues && entry.Value == null) {
                throw new PSArgumentException($"{parameterName} cannot contain null values.", parameterName);
            }

            pairs.Add((column!, entry.Value));
        }

        return pairs;
    }
}
