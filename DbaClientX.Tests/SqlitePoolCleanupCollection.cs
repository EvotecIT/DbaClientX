namespace DbaClientX.Tests;

/// <summary>Prevents process-wide SQLite pool cleanup from racing other fixtures' connection opens.</summary>
/// <remarks>
/// These fixtures clear all provider pools when deleting their temporary database files. They must finish
/// after parallel fixtures release their connections; unrelated query tests remain eligible for parallel execution.
/// </remarks>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SqlitePoolCleanupCollection
{
    /// <summary>The collection for fixtures that clear process-wide SQLite pools.</summary>
    public const string Name = "SQLite pool cleanup";
}
