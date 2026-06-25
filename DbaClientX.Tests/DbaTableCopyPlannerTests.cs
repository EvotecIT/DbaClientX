using DBAClientX.DataMovement;
using DBAClientX.Metadata;

namespace DbaClientX.Tests;

public class DbaTableCopyPlannerTests
{
    [Fact]
    public void BuildPlan_CreatesDestinationSafeDefinitionFromMetadata()
    {
        var sourceTables = new[]
        {
            new DbaTableInfo("main", "ProbeResults", DbaTableKind.Table),
            new DbaTableInfo("main", "ProbeResultsView", DbaTableKind.View)
        };
        var sourceColumns = new[]
        {
            Column("main", "ProbeResults", "ResultId", "INTEGER", 1, isIdentity: true),
            Column("main", "ProbeResults", "DisplayName", "TEXT", 2),
            Column("main", "ProbeResults", "IsMaintenance", "INTEGER", 3),
            Column("main", "ProbeResults", "SearchName", "TEXT", 4, generatedKind: "COMPUTED"),
            Column("main", "ProbeResults", "__MigrationRowId", "INTEGER", 5),
            Column("main", "ProbeResultsView", "Id", "INTEGER", 1)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "ProbeResults", "ResultId", "int", 1, isIdentity: true),
            Column("dbo", "ProbeResults", "ProbeDisplayName", "nvarchar(128)", 2),
            Column("dbo", "ProbeResults", "IsMaintenance", "bit", 3),
            Column("dbo", "ProbeResults", "SearchName", "nvarchar(128)", 4, generatedKind: "COMPUTED")
        };
        var indexes = new[]
        {
            new DbaIndexInfo("main", "ProbeResults", "pk_ProbeResults")
            {
                Column = "ResultId",
                Ordinal = 1,
                IsPrimaryKey = true,
                IsUnique = true
            }
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            indexes,
            destinationColumns,
            new DbaTableCopyPlanOptions
            {
                DestinationSchema = "dbo",
                ExcludeDestinationIdentityColumns = true,
                ExcludedColumns = new[] { "__MigrationRowId" },
                ColumnMappings = new Dictionary<string, string>
                {
                    ["DisplayName"] = "ProbeDisplayName"
                },
                ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>
                {
                    ["IsMaintenance"] = DbaTableCopyColumnType.Boolean
                },
                TableSourceOptions = new Dictionary<string, DbaTableCopySourceOptions>
                {
                    ["ProbeResults"] = new(
                        new[] { "DisplayName" },
                        new[] { "ResultId" },
                        DeduplicateCaseInsensitive: true)
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("main.ProbeResults", definition.SourceName);
        Assert.Equal("dbo.ProbeResults", definition.DestinationName);
        Assert.Equal("ProbeResults", definition.LogicalName);
        Assert.Equal(new[] { "ResultId" }, definition.OrderByColumns);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("ProbeDisplayName", definition.ColumnMappings["DisplayName"]);
        Assert.NotNull(definition.ColumnTypeConversions);
        Assert.Equal(DbaTableCopyColumnType.Boolean, definition.ColumnTypeConversions["IsMaintenance"]);
        Assert.NotNull(definition.SourceOptions);
        Assert.Equal(new[] { "DisplayName" }, definition.SourceOptions.DeduplicateByColumns);
        Assert.Equal(new[] { "ResultId" }, definition.SourceOptions.DeduplicateOrderByColumns);
        Assert.True(definition.SourceOptions.DeduplicateCaseInsensitive);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("ResultId", definition.ExcludedColumns);
        Assert.Contains("SearchName", definition.ExcludedColumns);
        Assert.Contains("__MigrationRowId", definition.ExcludedColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_ExcludesColumnsMissingFromDestinationAndWarns()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 2),
            Column("dbo", "Users", "Scratch", "nvarchar(128)", 3)
        };
        var destinationColumns = new[]
        {
            Column("archive", "Users", "Id", "int", 1),
            Column("archive", "Users", "DisplayName", "nvarchar(128)", 2)
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users")
            {
                Column = "Id",
                Ordinal = 1,
                IsPrimaryKey = true,
                IsUnique = true
            }
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            indexes,
            destinationColumns,
            new DbaTableCopyPlanOptions { DestinationSchema = "archive" });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("Scratch", definition.ExcludedColumns);
        var warning = Assert.Single(plan.Warnings);
        Assert.Equal("MissingDestinationColumn", warning.Code);
        Assert.Equal("Scratch", warning.ColumnName);
    }

    [Fact]
    public void BuildPlan_TreatsMissingDestinationMetadataTableAsNoWritableColumns()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 2)
        };
        var destinationColumns = new[]
        {
            Column("archive", "OtherUsers", "Id", "int", 1),
            Column("archive", "OtherUsers", "DisplayName", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions { DestinationSchema = "archive" });

        Assert.Empty(plan.Definitions);
        Assert.Contains(plan.Warnings, warning => warning.Code == "MissingDestinationColumn" && warning.ColumnName == "Id");
        Assert.Contains(plan.Warnings, warning => warning.Code == "MissingDestinationColumn" && warning.ColumnName == "DisplayName");
        Assert.Contains(plan.Warnings, warning => warning.Code == "NoWritableColumns");
    }

    [Fact]
    public void BuildPlan_UsesTableMappingsAndExplicitOrder()
    {
        var sourceTables = new[] { new DbaTableInfo("src", "Customers", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("src", "Customers", "CustomerId", "int", 1),
            Column("src", "Customers", "Name", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                TableMappings = new Dictionary<string, string>
                {
                    ["SRC.CUSTOMERS"] = "warehouse.Clients"
                },
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["customers"] = new[] { "CustomerId" }
                },
                TableColumnMappings = new Dictionary<string, IReadOnlyDictionary<string, string>>
                {
                    ["customers"] = new Dictionary<string, string>
                    {
                        ["Name"] = "DisplayName"
                    }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("src.Customers", definition.SourceName);
        Assert.Equal("warehouse.Clients", definition.DestinationName);
        Assert.Equal(new[] { "CustomerId" }, definition.OrderByColumns);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("DisplayName", definition.ColumnMappings["Name"]);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_WarnsWhenOrderCannotBeInferred()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "NoKey", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "NoKey", "Name", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns);

        var definition = Assert.Single(plan.Definitions);
        Assert.Null(definition.OrderByColumns);
        var warning = Assert.Single(plan.Warnings);
        Assert.Equal("NoOrderByColumns", warning.Code);
    }

    [Fact]
    public void BuildPlan_PreservesDestinationNameExclusionsAfterMapping()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1, isIdentity: true),
            Column("dbo", "Users", "Name", "nvarchar(128)", 2),
            Column("dbo", "Users", "UpdatedAt", "timestamp", 3)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                ColumnMappings = new Dictionary<string, string>
                {
                    ["Name"] = "DisplayName"
                },
                ExcludedColumns = new[] { "DisplayName" }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("DisplayName", definition.ExcludedColumns);
        Assert.Contains("Name", definition.ExcludedColumns);
        Assert.DoesNotContain("UpdatedAt", definition.ExcludedColumns);
    }

    [Fact]
    public void BuildPlan_ExcludesSqlServerTimestampRowversionShapeAsGenerated()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1),
            Column("dbo", "Users", "Version", "timestamp", 2, maxLength: 8)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns);

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("Version", definition.ExcludedColumns);
    }

    private static DbaColumnInfo Column(
        string schema,
        string table,
        string name,
        string dataType,
        int ordinal,
        bool? isIdentity = null,
        string? generatedKind = null,
        long? maxLength = null)
        => new(schema, table, name, dataType)
        {
            Ordinal = ordinal,
            IsIdentity = isIdentity,
            MaxLength = maxLength,
            GeneratedExpression = generatedKind == null ? null : name + " expression",
            GeneratedKind = generatedKind,
        };
}
