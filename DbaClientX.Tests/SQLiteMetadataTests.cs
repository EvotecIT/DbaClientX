using System;
using System.IO;
using DBAClientX.Metadata;
using Microsoft.Data.Sqlite;
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
            sqlite.ExecuteNonQuery(path, "CREATE TABLE ImplicitParents(Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
            sqlite.ExecuteNonQuery(path, "CREATE TABLE ImplicitChildren(ParentId INTEGER NOT NULL REFERENCES ImplicitParents);");
            sqlite.ExecuteNonQuery(path, "CREATE TABLE sqliteX(Id INTEGER PRIMARY KEY);");
            sqlite.ExecuteNonQuery(path, "CREATE INDEX IX_Users_Name ON Users(Name DESC);");
            sqlite.ExecuteNonQuery(path, "CREATE INDEX IX_Users_LowerName ON Users(lower(Name)) WHERE Name IS NOT NULL;");
            sqlite.ExecuteNonQuery(path, "CREATE VIEW ActiveUsers AS SELECT Id, Name FROM Users;");
            sqlite.ExecuteNonQuery(path, "CREATE VIRTUAL TABLE Docs USING fts5(Body);");

            var databases = sqlite.GetDatabases(path);
            var tables = sqlite.GetTables(path);
            var tablesOnly = sqlite.GetTables(path, includeViews: false);
            var tempTables = sqlite.GetTables(path, schema: "temp");
            var columns = sqlite.GetColumns(path, table: "Users");
            var tempColumns = sqlite.GetColumns(path, schema: "temp", table: "Users");
            var indexes = sqlite.GetIndexes(path, table: "Users");
            var tempIndexes = sqlite.GetIndexes(path, schema: "temp", table: "Users");
            var foreignKeys = sqlite.GetForeignKeys(path, table: "UserRoles");
            var tempForeignKeys = sqlite.GetForeignKeys(path, schema: "temp", table: "UserRoles");
            var implicitForeignKeys = sqlite.GetForeignKeys(path, table: "ImplicitChildren");
            var routines = sqlite.GetRoutines(path);

            Assert.Contains(databases, database =>
                database.Owner == "main" &&
                Path.GetFileName(database.Name) == Path.GetFileName(path));

            Assert.Contains(tables, table => table.Name == "Users" && table.Kind == DbaTableKind.Table);
            Assert.Contains(tables, table => table.Name == "sqliteX" && table.Kind == DbaTableKind.Table);
            Assert.Contains(tables, table => table.Name == "ActiveUsers" && table.Kind == DbaTableKind.View);
            Assert.DoesNotContain(tablesOnly, table => table.Kind == DbaTableKind.View);
            Assert.Empty(tempTables);

            var idColumn = Assert.Single(columns, column => column.Name == "Id");
            Assert.Equal("Users", idColumn.Table);
            Assert.Equal(1, idColumn.Ordinal);
            Assert.Empty(tempColumns);

            var nameColumn = Assert.Single(columns, column => column.Name == "Name");
            Assert.False(nameColumn.IsNullable);
            Assert.Equal("'unknown'", nameColumn.DefaultExpression);

            var slugColumn = Assert.Single(columns, column => column.Name == "Slug");
            Assert.Equal("STORED", slugColumn.GeneratedKind);
            Assert.DoesNotContain(tables, table => table.Name == "Docs_data");
            Assert.Contains(indexes, index => index.Name == "IX_Users_Name" && index.Column == "Name" && index.IsDescending == true && !index.IsPrimaryKey);
            Assert.Contains(indexes, index => index.Name == "IX_Users_LowerName" && index.Column is null && index.Expression != null && index.Expression.Contains("lower(Name)", StringComparison.OrdinalIgnoreCase) && index.FilterDefinition == "Name IS NOT NULL");
            Assert.Contains(indexes, index => index.Name == "pk_Users" && index.Column == "Id" && index.IsPrimaryKey);
            Assert.Empty(tempIndexes);
            Assert.Contains(foreignKeys, foreignKey =>
                foreignKey.Table == "UserRoles" &&
                foreignKey.Column == "UserId" &&
                foreignKey.ReferencedTable == "Users" &&
                foreignKey.ReferencedColumn == "Id" &&
                foreignKey.DeleteRule == "CASCADE");
            Assert.Empty(tempForeignKeys);
            Assert.Contains(implicitForeignKeys, foreignKey =>
                foreignKey.Column == "ParentId" &&
                foreignKey.ReferencedTable == "ImplicitParents" &&
                foreignKey.ReferencedColumn == "Id");
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

    [Fact]
    public void GetMetadataWithConnectionString_PreservesMemoryMode()
    {
        var database = "dbaclientx-memory-" + Guid.NewGuid().ToString("N");
        var connectionString = $"Data Source={database};Mode=Memory;Cache=Shared";

        using var keepAlive = new SqliteConnection(connectionString);
        keepAlive.Open();
        using (var command = keepAlive.CreateCommand())
        {
            command.CommandText = "CREATE TABLE Users(Id INTEGER PRIMARY KEY, Name TEXT);";
            command.ExecuteNonQuery();
        }

        using var sqlite = new DBAClientX.SQLite();

        var tables = sqlite.GetTablesWithConnectionString(connectionString);
        var columns = sqlite.GetColumnsWithConnectionString(connectionString, table: "Users");
        var indexes = sqlite.GetIndexesWithConnectionString(connectionString, table: "Users");

        Assert.Contains(tables, table => table.Name == "Users");
        Assert.Contains(columns, column => column.Name == "Id");
        Assert.Contains(indexes, index => index.IsPrimaryKey);
        Assert.False(File.Exists(database));
    }

    [Fact]
    public void GetMetadataWithConnectionString_MissingFileDoesNotCreateDatabase()
    {
        var path = Path.Join(Path.GetTempPath(), "dbaclientx-missing-metadata-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var sqlite = new DBAClientX.SQLite();

            Assert.Throws<SqliteException>(() => sqlite.GetTablesWithConnectionString($"Data Source={path}"));
            Assert.False(File.Exists(path));
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
