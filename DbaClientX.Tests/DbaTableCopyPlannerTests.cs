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
        Assert.Equal("main.\"ProbeResults\"", definition.SourceName);
        Assert.Equal("dbo.\"ProbeResults\"", definition.DestinationName);
        Assert.Equal("ProbeResults", definition.LogicalName);
        Assert.Equal(new[] { "\"ResultId\"" }, definition.OrderByColumns);
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
                    ["src.Customers"] = "warehouse.Clients"
                },
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Customers"] = new[] { "CustomerId" }
                },
                TableColumnMappings = new Dictionary<string, IReadOnlyDictionary<string, string>>
                {
                    ["Customers"] = new Dictionary<string, string>
                    {
                        ["Name"] = "DisplayName"
                    }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("src.\"Customers\"", definition.SourceName);
        Assert.Equal("warehouse.Clients", definition.DestinationName);
        Assert.Equal(new[] { "CustomerId" }, definition.OrderByColumns);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("DisplayName", definition.ColumnMappings["Name"]);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_PreservesColumnMappingComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("archive", "Users", "ProbeDisplayName", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                DestinationSchema = "archive",
                ColumnMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["displayname"] = "ProbeDisplayName"
                },
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Users"] = new[] { "DisplayName" }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("ProbeDisplayName", definition.ColumnMappings["DisplayName"]);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_PreservesGlobalColumnMappingComparerWhenScopedMappingsExist()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1),
            Column("dbo", "Users", "Department", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                ColumnMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["displayname"] = "display_name"
                },
                TableColumnMappings = new Dictionary<string, IReadOnlyDictionary<string, string>>
                {
                    ["dbo.Users"] = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["Department"] = "department_name"
                    }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("display_name", definition.ColumnMappings["DisplayName"]);
        Assert.Equal("department_name", definition.ColumnMappings["Department"]);
    }

    [Fact]
    public void BuildPlan_PreservesScopedOnlyColumnMappingComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "Users", "display_name", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                MatchDestinationColumns = true,
                TableColumnMappings = new Dictionary<string, IReadOnlyDictionary<string, string>>
                {
                    ["dbo.Users"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["displayname"] = "display_name"
                    }
                },
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Users"] = new[] { "DisplayName" }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("display_name", definition.ColumnMappings["DisplayName"]);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_PreservesColumnTypeConversionComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "IsEnabled", "int", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase)
                {
                    ["isenabled"] = DbaTableCopyColumnType.Boolean
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnTypeConversions);
        Assert.Equal(DbaTableCopyColumnType.Boolean, definition.ColumnTypeConversions["IsEnabled"]);
    }

    [Fact]
    public void BuildPlan_PreservesGlobalColumnTypeConversionComparerWhenScopedMappingsExist()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "IsEnabled", "int", 1),
            Column("dbo", "Users", "CreatedUtc", "datetime2", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase)
                {
                    ["isenabled"] = DbaTableCopyColumnType.Boolean
                },
                TableColumnTypeConversions = new Dictionary<string, IReadOnlyDictionary<string, DbaTableCopyColumnType>>
                {
                    ["dbo.Users"] = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.Ordinal)
                    {
                        ["CreatedUtc"] = DbaTableCopyColumnType.DateTime
                    }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnTypeConversions);
        Assert.Equal(DbaTableCopyColumnType.Boolean, definition.ColumnTypeConversions["IsEnabled"]);
        Assert.Equal(DbaTableCopyColumnType.DateTime, definition.ColumnTypeConversions["CreatedUtc"]);
    }

    [Fact]
    public void BuildPlan_PreservesExcludedColumnComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1),
            Column("dbo", "Users", "Helper", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                ExcludedColumns = new HashSet<string>(new[] { "helper" }, StringComparer.OrdinalIgnoreCase)
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("Helper", definition.ExcludedColumns);
        Assert.DoesNotContain("DisplayName", definition.ExcludedColumns);
    }

    [Fact]
    public void BuildPlan_MatchesExcludedColumnsWithProviderDefaultComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1),
            Column("dbo", "Users", "Helper", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.SqlServer,
                ExcludedColumns = new[] { "helper" }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("Helper", definition.ExcludedColumns);
        Assert.DoesNotContain("DisplayName", definition.ExcludedColumns);
    }

    [Fact]
    public void BuildPlan_PreservesGlobalExcludedColumnComparerWhenScopedMappingsExist()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Helper", "nvarchar(128)", 1),
            Column("dbo", "Users", "Scratch", "nvarchar(128)", 2),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 3)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                ExcludedColumns = new HashSet<string>(new[] { "helper" }, StringComparer.OrdinalIgnoreCase),
                TableExcludedColumns = new Dictionary<string, IReadOnlyCollection<string>>
                {
                    ["dbo.Users"] = new HashSet<string>(new[] { "Scratch" }, StringComparer.Ordinal)
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("Helper", definition.ExcludedColumns);
        Assert.Contains("Scratch", definition.ExcludedColumns);
        Assert.DoesNotContain("DisplayName", definition.ExcludedColumns);
    }

    [Fact]
    public void BuildPlan_PreservesScopedOnlyExcludedColumnComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Helper", "nvarchar(128)", 1),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                TableExcludedColumns = new Dictionary<string, IReadOnlyCollection<string>>
                {
                    ["dbo.Users"] = new HashSet<string>(new[] { "helper" }, StringComparer.OrdinalIgnoreCase)
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ExcludedColumns);
        Assert.Contains("Helper", definition.ExcludedColumns);
        Assert.DoesNotContain("DisplayName", definition.ExcludedColumns);
    }

    [Fact]
    public void BuildPlan_PreservesQualifiedTableMappingWhenDestinationSchemaIsSet()
    {
        var sourceTables = new[] { new DbaTableInfo("src", "Customers", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("src", "Customers", "CustomerId", "int", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                DestinationSchema = "archive",
                TableMappings = new Dictionary<string, string>
                {
                    ["src.Customers"] = "warehouse.Clients"
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("warehouse.Clients", definition.DestinationName);
    }

    [Fact]
    public void BuildPlan_MatchesThreePartDestinationMappingsToSchemaTableMetadata()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Rows", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Rows", "Id", "int", 1),
            Column("dbo", "Rows", "DisplayName", "nvarchar(128)", 2)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "Rows", "Id", "int", 1),
            Column("dbo", "Rows", "DisplayName", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                TableMappings = new Dictionary<string, string>
                {
                    ["dbo.Rows"] = "Archive.dbo.Rows"
                },
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Rows"] = new[] { "Id" }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("Archive.dbo.Rows", definition.DestinationName);
        Assert.Null(definition.ExcludedColumns);
        Assert.Empty(plan.Warnings);
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.SqlServer)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [InlineData(DbaTableCopyProvider.SQLite)]
    public void BuildPlan_KeepsNonSimpleBulkColumnNamesLiteralForNonFoldingProviders(DbaTableCopyProvider provider)
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Rows", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Rows", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "Rows", "Display Name", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = provider,
                DestinationColumnNameComparer = StringComparer.OrdinalIgnoreCase,
                ColumnMappings = new Dictionary<string, string>
                {
                    ["DisplayName"] = "Display Name"
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("Display Name", definition.ColumnMappings["DisplayName"]);
    }

    [Fact]
    public void BuildPlan_QuotesNonSimpleBulkColumnNamesForPostgreSql()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "rows", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "rows", "displayname", "text", 1)
        };
        var destinationColumns = new[]
        {
            Column("public", "rows", "Display Name", "text", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.PostgreSql,
                ColumnMappings = new Dictionary<string, string>
                {
                    ["displayname"] = "Display Name"
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("\"Display Name\"", definition.ColumnMappings["displayname"]);
    }

    [Fact]
    public void BuildPlan_QuotesPostgreSqlSourceOnlyMixedCaseColumns()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "users", "Id", "integer", 1, isIdentity: true),
            Column("public", "users", "CreatedUtc", "timestamp", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions { IdentifierProvider = DbaTableCopyProvider.PostgreSql });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("\"Id\"", definition.ColumnMappings["Id"]);
        Assert.Equal("\"CreatedUtc\"", definition.ColumnMappings["CreatedUtc"]);
        Assert.Equal(new[] { "\"Id\"" }, definition.OrderByColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_DoesNotApplyCaseDifferentScopedTableMappings()
    {
        var sourceTables = new[]
        {
            new DbaTableInfo("public", "Users", DbaTableKind.Table),
            new DbaTableInfo("public", "users", DbaTableKind.Table)
        };
        var sourceColumns = new[]
        {
            Column("public", "Users", "Id", "integer", 1),
            Column("public", "users", "id", "integer", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                TableMappings = new Dictionary<string, string>
                {
                    ["Users"] = "ArchiveUsers"
                }
            });

        Assert.Collection(
            plan.Definitions,
            first => Assert.Equal("public.\"ArchiveUsers\"", first.DestinationName),
            second => Assert.Equal("public.users", second.DestinationName));
    }

    [Fact]
    public void BuildPlan_FiltersSourceSchemaCaseSensitively()
    {
        var sourceTables = new[]
        {
            new DbaTableInfo("Tenant", "Rows", DbaTableKind.Table),
            new DbaTableInfo("tenant", "Rows", DbaTableKind.Table)
        };
        var sourceColumns = new[]
        {
            Column("Tenant", "Rows", "Id", "int", 1),
            Column("tenant", "Rows", "id", "int", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions { SourceSchema = "tenant" });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("tenant.\"Rows\"", definition.SourceName);
    }

    [Fact]
    public void BuildPlan_FiltersOracleSourceSchemaWithProviderFolding()
    {
        var sourceTables = new[] { new DbaTableInfo("APP", "ROWS", DbaTableKind.Table) };
        var sourceColumns = new[] { Column("APP", "ROWS", "ID", "number", 1) };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                SourceSchema = "app",
                IdentifierProvider = DbaTableCopyProvider.Oracle
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("APP.\"ROWS\"", definition.SourceName);
    }

    [Fact]
    public void BuildPlan_FiltersSqlServerSourceSchemaCaseInsensitively()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Rows", DbaTableKind.Table) };
        var sourceColumns = new[] { Column("dbo", "Rows", "Id", "int", 1) };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.SqlServer,
                SourceSchema = "DBO"
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("dbo.Rows", definition.SourceName);
    }

    [Fact]
    public void BuildPlan_DelimitsMetadataNamesThatContainDots()
    {
        var sourceTables = new[] { new DbaTableInfo("tenant.v1", "Probe.Results", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("tenant.v1", "Probe.Results", "ResultId", "int", 1, isIdentity: true)
        };
        var destinationColumns = new[]
        {
            Column("archive.v1", "Probe.Results", "ResultId", "int", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions { DestinationSchema = "archive.v1" });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("\"tenant.v1\".\"Probe.Results\"", definition.SourceName);
        Assert.Equal("\"archive.v1\".\"Probe.Results\"", definition.DestinationName);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_AppliesDestinationSchemaToDelimitedMappedTableNamesWithDots()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1)
        };
        var destinationColumns = new[]
        {
            Column("archive", "Rows.Current", "Id", "int", 1)
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
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                DestinationSchema = "archive",
                TableMappings = new Dictionary<string, string>
                {
                    ["Users"] = "\"Rows.Current\""
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("archive.\"Rows.Current\"", definition.DestinationName);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_MatchesDestinationMetadataColumnsCaseSensitively()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("archive", "Users", "displayname", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions { DestinationSchema = "archive" });

        Assert.Empty(plan.Definitions);
        Assert.Contains(plan.Warnings, warning => warning.Code == "MissingDestinationColumn" && warning.ColumnName == "DisplayName");
        Assert.Contains(plan.Warnings, warning => warning.Code == "NoWritableColumns");
    }

    [Fact]
    public void BuildPlan_UsesConfiguredDestinationColumnComparer()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("archive", "Users", "displayname", "text", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                DestinationSchema = "archive",
                DestinationColumnNameComparer = StringComparer.OrdinalIgnoreCase
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Null(definition.ExcludedColumns);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("displayname", definition.ColumnMappings["DisplayName"]);
        Assert.DoesNotContain(plan.Warnings, warning => warning.Code == "MissingDestinationColumn");
    }

    [Fact]
    public void BuildPlan_KeepsTableMetadataGroupsCaseSensitive()
    {
        var sourceTables = new[] { new DbaTableInfo("archive", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("archive", "Users", "Name", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("archive", "users", "Name", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns, destinationColumns: destinationColumns);

        Assert.Empty(plan.Definitions);
        Assert.Contains(plan.Warnings, warning => warning.Code == "MissingDestinationColumn" && warning.ColumnName == "Name");
        Assert.Contains(plan.Warnings, warning => warning.Code == "NoWritableColumns");
    }

    [Fact]
    public void BuildPlan_MatchesSqlServerTableMetadataGroupsCaseInsensitively()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "users", "displayname", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.SqlServer,
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Users"] = new[] { "DisplayName" }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Null(definition.ExcludedColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_MatchesSQLiteTableMetadataGroupsCaseInsensitively()
    {
        var sourceTables = new[] { new DbaTableInfo("main", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("main", "Users", "DisplayName", "TEXT", 1)
        };
        var destinationColumns = new[]
        {
            Column("main", "users", "displayname", "TEXT", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.SQLite,
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Users"] = new[] { "DisplayName" }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Null(definition.ExcludedColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_MatchesSqlServerDestinationMetadataColumnsCaseInsensitivelyByDefault()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 1)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "Users", "displayname", "nvarchar(128)", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.SqlServer
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("displayname", definition.ColumnMappings["DisplayName"]);
        Assert.DoesNotContain(plan.Warnings, warning => warning.Code == "MissingDestinationColumn");
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.MySql)]
    [InlineData(DbaTableCopyProvider.SQLite)]
    public void BuildPlan_MatchesProviderDestinationMetadataColumnsCaseInsensitivelyByDefault(DbaTableCopyProvider provider)
    {
        var sourceTables = new[] { new DbaTableInfo("main", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("main", "Users", "DisplayName", "TEXT", 1)
        };
        var destinationColumns = new[]
        {
            Column("main", "Users", "displayname", "TEXT", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = provider
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("displayname", definition.ColumnMappings["DisplayName"]);
        Assert.DoesNotContain(plan.Warnings, warning => warning.Code == "MissingDestinationColumn");
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.Oracle)]
    public void BuildPlan_QuotesExplicitProviderOrderColumns(DbaTableCopyProvider provider)
    {
        var sourceTables = new[] { new DbaTableInfo("app", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("app", "Users", "CreatedUtc", "timestamp", 1)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = provider,
                OrderByColumns = new Dictionary<string, IReadOnlyList<string>>
                {
                    ["Users"] = new[] { "CreatedUtc" }
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal(new[] { "\"CreatedUtc\"" }, definition.OrderByColumns);
        Assert.Empty(plan.Warnings);
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.Oracle)]
    public void BuildPlan_QuotesProviderDeduplicationColumns(DbaTableCopyProvider provider)
    {
        var sourceTables = new[] { new DbaTableInfo("app", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("app", "Users", "Email", "text", 1),
            Column("app", "Users", "CreatedUtc", "timestamp", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = provider,
                SourceOptions = new DbaTableCopySourceOptions(
                    new[] { "Email" },
                    new[] { "CreatedUtc" })
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.SourceOptions);
        Assert.Equal(new[] { "\"Email\"" }, definition.SourceOptions.DeduplicateByColumns);
        Assert.Equal(new[] { "\"CreatedUtc\"" }, definition.SourceOptions.DeduplicateOrderByColumns);
    }

    [Fact]
    public void BuildPlan_QuotesMixedCaseMetadataOrderColumns()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "Users", "Id", "integer", 1),
            Column("public", "Users", "name", "text", 2)
        };
        var indexes = new[]
        {
            new DbaIndexInfo("public", "Users", "Users_pkey")
            {
                Column = "Id",
                Ordinal = 1,
                IsPrimaryKey = true,
                IsUnique = true
            }
        };

        var plan = DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns, indexes);

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal(new[] { "\"Id\"" }, definition.OrderByColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_MatchesIndexColumnsWithProviderCasingRules()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 2)
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users")
            {
                Column = "id",
                Ordinal = 1,
                IsPrimaryKey = true,
                IsUnique = true
            }
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            indexes,
            options: new DbaTableCopyPlanOptions { IdentifierProvider = DbaTableCopyProvider.SqlServer });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal(new[] { "id" }, definition.OrderByColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_QuotesMixedCaseIdentityOrderColumns()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "Users", "Id", "integer", 1, isIdentity: true),
            Column("public", "Users", "name", "text", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns);

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal(new[] { "\"Id\"" }, definition.OrderByColumns);
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

    [Fact]
    public void BuildPlan_PreservesCaseForMetadataTableNames()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "Users", "Id", "int", 1, isIdentity: true)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns);

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("public.\"Users\"", definition.SourceName);
        Assert.Equal("public.\"Users\"", definition.DestinationName);
    }

    [Fact]
    public void BuildPlan_QuotesLowercaseOracleMetadataIdentifiers()
    {
        var sourceTables = new[] { new DbaTableInfo("app", "orders", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("app", "orders", "id", "NUMBER", 1, isIdentity: true),
            Column("app", "orders", "name", "VARCHAR2", 2)
        };
        var destinationColumns = new[]
        {
            Column("app", "orders", "id", "NUMBER", 1),
            Column("app", "orders", "name", "VARCHAR2", 2)
        };
        var indexes = new[]
        {
            new DbaIndexInfo("app", "orders", "pk_orders")
            {
                Column = "id",
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
                IdentifierProvider = DbaTableCopyProvider.Oracle
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("\"app\".\"orders\"", definition.SourceName);
        Assert.Equal("\"app\".\"orders\"", definition.DestinationName);
        Assert.Equal(new[] { "\"id\"" }, definition.OrderByColumns);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_QuotesPostgreSqlDestinationMetadataColumnsThatPreserveCase()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "users", "id", "integer", 1, isIdentity: true),
            Column("public", "users", "displayname", "text", 2)
        };
        var destinationColumns = new[]
        {
            Column("public", "users", "id", "integer", 1),
            Column("public", "users", "DisplayName", "text", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions
            {
                IdentifierProvider = DbaTableCopyProvider.PostgreSql,
                ColumnMappings = new Dictionary<string, string>
                {
                    ["displayname"] = "DisplayName"
                }
            });

        var definition = Assert.Single(plan.Definitions);
        Assert.NotNull(definition.ColumnMappings);
        Assert.Equal("\"DisplayName\"", definition.ColumnMappings["displayname"]);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_KeepsSqlServerDestinationMetadataColumnsUnquoted()
    {
        var sourceTables = new[] { new DbaTableInfo("dbo", "Users", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1, isIdentity: true),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 2)
        };
        var destinationColumns = new[]
        {
            Column("dbo", "Users", "Id", "int", 1),
            Column("dbo", "Users", "DisplayName", "nvarchar(128)", 2)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            destinationColumns: destinationColumns,
            options: new DbaTableCopyPlanOptions { IdentifierProvider = DbaTableCopyProvider.SqlServer });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("dbo.Users", definition.SourceName);
        Assert.Equal("dbo.Users", definition.DestinationName);
        Assert.Equal(new[] { "Id" }, definition.OrderByColumns);
        Assert.Null(definition.ColumnMappings);
        Assert.Empty(plan.Warnings);
    }

    [Fact]
    public void BuildPlan_QuotesProviderReservedMetadataIdentifiers()
    {
        var sourceTables = new[] { new DbaTableInfo("public", "order", DbaTableKind.Table) };
        var sourceColumns = new[]
        {
            Column("public", "order", "user", "text", 1, isIdentity: true)
        };

        var plan = DbaTableCopyPlanner.BuildPlan(
            sourceTables,
            sourceColumns,
            options: new DbaTableCopyPlanOptions { IdentifierProvider = DbaTableCopyProvider.PostgreSql });

        var definition = Assert.Single(plan.Definitions);
        Assert.Equal("public.\"order\"", definition.SourceName);
        Assert.Equal("public.\"order\"", definition.DestinationName);
        Assert.Equal(new[] { "\"user\"" }, definition.OrderByColumns);
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
