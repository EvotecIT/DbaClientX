using System;
using System.IO;
using DBAClientX.Metadata;
using Xunit;

namespace DbaClientX.Tests;

public class SQLiteMetadataTests
{
    [Fact]
    public void GetMetadata_ReturnsTablesColumnsAndIndexes()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Users(Id INTEGER PRIMARY KEY, Name TEXT NOT NULL DEFAULT 'unknown');");
            sqlite.ExecuteNonQuery(path, "CREATE INDEX IX_Users_Name ON Users(Name);");
            sqlite.ExecuteNonQuery(path, "CREATE VIEW ActiveUsers AS SELECT Id, Name FROM Users;");

            var databases = sqlite.GetDatabases(path);
            var tables = sqlite.GetTables(path);
            var tablesOnly = sqlite.GetTables(path, includeViews: false);
            var columns = sqlite.GetColumns(path, table: "Users");
            var indexes = sqlite.GetIndexes(path, table: "Users");

            Assert.Contains(databases, database => database.Owner == "main" && database.Name == path);

            Assert.Contains(tables, table => table.Name == "Users" && table.Kind == DbaTableKind.Table);
            Assert.Contains(tables, table => table.Name == "ActiveUsers" && table.Kind == DbaTableKind.View);
            Assert.DoesNotContain(tablesOnly, table => table.Kind == DbaTableKind.View);

            var idColumn = Assert.Single(columns, column => column.Name == "Id");
            Assert.Equal("Users", idColumn.Table);
            Assert.Equal(1, idColumn.Ordinal);

            var nameColumn = Assert.Single(columns, column => column.Name == "Name");
            Assert.False(nameColumn.IsNullable);
            Assert.Equal("'unknown'", nameColumn.DefaultExpression);

            Assert.Contains(indexes, index => index.Name == "IX_Users_Name" && index.Column == "Name" && !index.IsPrimaryKey);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public void GetColumns_WithMissingTable_ReturnsEmptyCollection()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Users(Id INTEGER PRIMARY KEY);");

            var columns = sqlite.GetColumns(path, table: "Missing");

            Assert.Empty(columns);
        }
        finally
        {
            Cleanup(path);
        }
    }

    private static void Cleanup(string path)
    {
        TryDelete(path);
        TryDelete(path + "-wal");
        TryDelete(path + "-shm");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
