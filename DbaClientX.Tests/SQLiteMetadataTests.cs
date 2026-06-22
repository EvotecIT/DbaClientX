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
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Users(Id INTEGER PRIMARY KEY, Name TEXT NOT NULL DEFAULT 'unknown', Slug TEXT GENERATED ALWAYS AS (lower(Name)) STORED);");
            sqlite.ExecuteNonQuery(path, "CREATE TABLE Roles(Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
            sqlite.ExecuteNonQuery(path, "CREATE TABLE UserRoles(UserId INTEGER NOT NULL, RoleId INTEGER NOT NULL, CONSTRAINT FK_UserRoles_Users FOREIGN KEY(UserId) REFERENCES Users(Id) ON DELETE CASCADE, FOREIGN KEY(RoleId) REFERENCES Roles(Id));");
            sqlite.ExecuteNonQuery(path, "CREATE INDEX IX_Users_Name ON Users(Name DESC);");
            sqlite.ExecuteNonQuery(path, "CREATE VIEW ActiveUsers AS SELECT Id, Name FROM Users;");
            sqlite.ExecuteNonQuery(path, "CREATE VIRTUAL TABLE Docs USING fts5(Body);");

            var databases = sqlite.GetDatabases(path);
            var tables = sqlite.GetTables(path);
            var tablesOnly = sqlite.GetTables(path, includeViews: false);
            var columns = sqlite.GetColumns(path, table: "Users");
            var indexes = sqlite.GetIndexes(path, table: "Users");
            var foreignKeys = sqlite.GetForeignKeys(path, table: "UserRoles");
            var routines = sqlite.GetRoutines(path);

            Assert.Contains(databases, database =>
                database.Owner == "main" &&
                Path.GetFileName(database.Name) == Path.GetFileName(path));

            Assert.Contains(tables, table => table.Name == "Users" && table.Kind == DbaTableKind.Table);
            Assert.Contains(tables, table => table.Name == "ActiveUsers" && table.Kind == DbaTableKind.View);
            Assert.DoesNotContain(tablesOnly, table => table.Kind == DbaTableKind.View);

            var idColumn = Assert.Single(columns, column => column.Name == "Id");
            Assert.Equal("Users", idColumn.Table);
            Assert.Equal(1, idColumn.Ordinal);

            var nameColumn = Assert.Single(columns, column => column.Name == "Name");
            Assert.False(nameColumn.IsNullable);
            Assert.Equal("'unknown'", nameColumn.DefaultExpression);

            Assert.Contains(columns, column => column.Name == "Slug");
            Assert.DoesNotContain(tables, table => table.Name == "Docs_data");
            Assert.Contains(indexes, index => index.Name == "IX_Users_Name" && index.Column == "Name" && index.IsDescending == true && !index.IsPrimaryKey);
            Assert.Contains(indexes, index => index.Name == "pk_Users" && index.Column == "Id" && index.IsPrimaryKey);
            Assert.Contains(foreignKeys, foreignKey =>
                foreignKey.Table == "UserRoles" &&
                foreignKey.Column == "UserId" &&
                foreignKey.ReferencedTable == "Users" &&
                foreignKey.ReferencedColumn == "Id" &&
                foreignKey.DeleteRule == "CASCADE");
            Assert.Empty(routines);
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
