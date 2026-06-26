using System.Data;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public class DbaTableCopyEngineTests
{
    [Fact]
    public async Task CopyAsync_CopiesPagedRowsClearsDestinationAndReportsProgress()
    {
        var sourceTable = CreateRows(7);
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();
        var progress = new List<DbaTableCopyProgress>();

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }, "Rows") },
            new DbaTableCopyOptions
            {
                PageSize = 3,
                BatchSize = 2,
                BulkCopyTimeout = 30,
                ClearDestination = true,
                Progress = progress.Add
            });

        Assert.True(destination.ClearCalled);
        Assert.Equal(new[] { 3, 3, 3 }, source.RequestedPageSizes);
        Assert.Equal(new long[] { 0, 3, 6 }, source.RequestedOffsets);
        Assert.Equal(7, destination.Rows.Rows.Count);
        Assert.Equal(7, result.CopiedRows);
        Assert.Equal(7, result.SourceRows);
        Assert.Equal(7, result.DestinationRows);
        Assert.True(result.Verified);
        Assert.Collection(
            progress,
            first => AssertProgress(first, 3, 7, 3),
            second => AssertProgress(second, 6, 7, 3),
            third => AssertProgress(third, 7, 7, 1));
    }

    [Fact]
    public async Task CopyAsync_ReportsVerificationMismatch()
    {
        var source = new MemoryTableCopySource(CreateRows(2));
        var destination = new MemoryTableCopyDestination(destinationRowCountOverride: 1);

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") });

        var tableResult = Assert.Single(result.Tables);
        Assert.False(result.Verified);
        Assert.False(tableResult.Verified);
        Assert.Equal(2, tableResult.SourceRows);
        Assert.Equal(2, tableResult.CopiedRows);
        Assert.Equal(1, tableResult.DestinationRows);
    }

    [Fact]
    public async Task CopyAsync_SkipsReadWhenSourceCountIsZero()
    {
        var source = new MemoryTableCopySource(CreateRows(0));
        var destination = new MemoryTableCopyDestination();

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new DbaTableCopyOptions { ClearDestination = true });

        Assert.Equal(0, source.ReadCalls);
        Assert.True(destination.ClearCalled);
        Assert.True(result.Verified);
        Assert.Equal(0, result.SourceRows);
        Assert.Equal(0, result.CopiedRows);
        Assert.Equal(0, result.DestinationRows);
    }

    [Fact]
    public async Task CopyAsync_WritesSchemaPageWhenDestinationRequestsEmptyPages()
    {
        var source = new MemoryTableCopySource(CreateRows(0));
        var destination = new MemoryTableCopyDestination { WriteEmptyPages = true };

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") });

        Assert.Equal(1, source.ReadCalls);
        Assert.Equal(new long[] { 0 }, source.RequestedOffsets);
        Assert.Equal(new[] { "DestinationRows" }, destination.WriteOrder);
        Assert.Equal(2, destination.Rows.Columns.Count);
        Assert.Equal(0, destination.Rows.Rows.Count);
        Assert.True(result.Verified);
        Assert.Equal(0, result.SourceRows);
        Assert.Equal(0, result.CopiedRows);
        Assert.Equal(0, result.DestinationRows);
    }

    [Fact]
    public async Task CopyAsync_DefersInitialDestinationCountWhenDestinationCreatesOnWrite()
    {
        var source = new MemoryTableCopySource(CreateRows(2));
        var destination = new MemoryTableCopyDestination
        {
            ThrowOnCountBeforeWrite = true,
            WriteEmptyPages = true
        };

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") });

        Assert.Equal(1, destination.CountCalls);
        Assert.Equal(2, result.SourceRows);
        Assert.Equal(2, result.CopiedRows);
        Assert.Equal(2, result.DestinationRows);
        Assert.True(result.Verified);
    }

    [Fact]
    public async Task CopyAsync_ClearsDestinationTablesInReverseOrderBeforeCopying()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition("ProbeResults", "ProbeResults"),
                new DbaTableCopyDefinition("ProbeResultMetadata", "ProbeResultMetadata")
            },
            new DbaTableCopyOptions
            {
                ClearDestination = true,
                VerifyRowCounts = false
            });

        Assert.Equal(new[] { "ProbeResultMetadata", "ProbeResults" }, destination.ClearOrder);
        Assert.Equal(new[] { "ProbeResults", "ProbeResultMetadata" }, destination.WriteOrder);
    }

    [Fact]
    public async Task CopyAsync_DoesNotClearDestinationWhenSourceCountFails()
    {
        var source = new MemoryTableCopySource(CreateRows(1)) { ThrowOnCountRows = true };
        var destination = new MemoryTableCopyDestination();

        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_DoesNotClearDestinationWhenFirstSourcePageFails()
    {
        var source = new MemoryTableCopySource(CreateRows(1)) { ThrowOnRead = true };
        var destination = new MemoryTableCopyDestination();

        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_DoesNotClearDestinationWhenFirstSourcePageTransformFails()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["DisplayName"] = "Id"
                    })
            },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.Contains("duplicate destination column 'Id'", exception.Message);
        Assert.Contains("Id", exception.Message);
        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_DoesNotClearDestinationWhenFirstSourcePageHasNoDestinationColumns()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ExcludedColumns: new[] { "Id", "DisplayName" })
            },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.Contains("produced no destination columns", exception.Message);
        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_DoesNotClearDestinationWhenDestinationPagePreflightFails()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination
        {
            ThrowOnValidatePage = true
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.Equal(1, destination.ValidatePageCalls);
        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_DoesNotPartiallyClearDestinationWhenDestinationPreflightFails()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination
        {
            ThrowOnCountDestinationName = "MissingRows"
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition("MissingRows", "MissingRows"),
                new DbaTableCopyDefinition("ExistingRows", "ExistingRows")
            },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_RejectsDuplicateClearDestinationsBeforePreflight()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition("UsersA", "dbo.Users"),
                new DbaTableCopyDefinition("UsersB", "[dbo].[Users]")
            },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.Contains("ClearDestination cannot be used with multiple definitions targeting destination", exception.Message);
        Assert.Equal(0, source.CountCalls);
        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_AllowsCaseDistinctClearDestinations()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition("\"Rows\"", "\"Rows\""),
                new DbaTableCopyDefinition("\"rows\"", "\"rows\"")
            },
            new DbaTableCopyOptions
            {
                ClearDestination = true,
                VerifyRowCounts = false
            });

        Assert.Equal(new[] { "\"rows\"", "\"Rows\"" }, destination.ClearOrder);
    }

    [Fact]
    public async Task CopyAsync_VerifiesAppendAgainstInitialDestinationRows()
    {
        var source = new MemoryTableCopySource(CreateRows(2));
        var destination = new MemoryTableCopyDestination();
        destination.Rows.Columns.Add("Id", typeof(int));
        destination.Rows.Columns.Add("DisplayName", typeof(string));
        destination.Rows.Rows.Add(100, "Existing");

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) },
            new DbaTableCopyOptions
            {
                ClearDestination = false,
                VerifyRowCounts = true
            });

        Assert.True(result.Verified);
        Assert.Equal(2, result.SourceRows);
        Assert.Equal(2, result.CopiedRows);
        Assert.Equal(3, result.DestinationRows);
    }

    [Fact]
    public async Task CopyAsync_AppendVerificationRequiresCopiedRowsToMatchKnownSourceRows()
    {
        var source = new MemoryTableCopySource(CreateRows(2))
        {
            SourceRowCountOverride = 3
        };
        var destination = new MemoryTableCopyDestination();
        destination.Rows.Columns.Add("Id", typeof(int));
        destination.Rows.Columns.Add("DisplayName", typeof(string));
        destination.Rows.Rows.Add(100, "Existing");

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) },
            new DbaTableCopyOptions
            {
                ClearDestination = false,
                VerifyRowCounts = true
            });

        Assert.False(result.Verified);
        Assert.Equal(3, result.SourceRows);
        Assert.Equal(2, result.CopiedRows);
        Assert.Equal(3, result.DestinationRows);
    }

    [Fact]
    public async Task CopyAsync_AppliesColumnMappingsExclusionsAndTypeConversions()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("Id", typeof(long));
        sourceTable.Columns.Add("DisplayName", typeof(string));
        sourceTable.Columns.Add("IsMaintenance", typeof(int));
        sourceTable.Columns.Add("DurationText", typeof(string));
        sourceTable.Columns.Add("__MigrationRowId", typeof(long));
        sourceTable.Rows.Add(1L, "First", 1, "42", 100L);
        sourceTable.Rows.Add(2L, "Second", 0, "55", 101L);
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["DisplayName"] = "Name"
                    },
                    ExcludedColumns: new[] { "__MigrationRowId" },
                    ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType>
                    {
                        ["IsMaintenance"] = DbaTableCopyColumnType.Boolean,
                        ["DurationText"] = DbaTableCopyColumnType.Int32
                    })
            });

        Assert.DoesNotContain("__MigrationRowId", destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.Contains("Name", destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.Equal(typeof(bool), destination.Rows.Columns["IsMaintenance"]!.DataType);
        Assert.Equal(typeof(int), destination.Rows.Columns["DurationText"]!.DataType);
        Assert.Equal(true, destination.Rows.Rows[0]["IsMaintenance"]);
        Assert.Equal(false, destination.Rows.Rows[1]["IsMaintenance"]);
        Assert.Equal(42, destination.Rows.Rows[0]["DurationText"]);
        Assert.Equal("First", destination.Rows.Rows[0]["Name"]);
    }

    [Fact]
    public async Task CopyAsync_ExcludesMappedColumnsByDestinationName()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("Id", typeof(long));
        sourceTable.Columns.Add("DisplayName", typeof(string));
        sourceTable.Rows.Add(1L, "First");
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["DisplayName"] = "Name"
                    },
                    ExcludedColumns: new[] { "Name" })
            });

        Assert.DoesNotContain("DisplayName", destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.DoesNotContain("Name", destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.Contains("Id", destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public async Task CopyAsync_AppliesColumnMappingsWithExactSourceColumnCase()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("Name", typeof(string));
        sourceTable.Columns.Add("name", typeof(string));
        sourceTable.Rows.Add("Upper", "Lower");
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["Name"] = "FullName"
                    })
            });

        Assert.Equal(new[] { "FullName", "name" }, destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.Equal("Upper", destination.Rows.Rows[0]["FullName"]);
        Assert.Equal("Lower", destination.Rows.Rows[0]["name"]);
    }

    [Fact]
    public async Task CopyAsync_ExcludesColumnsUsingSuppliedCollectionComparer()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("Helper", typeof(string));
        sourceTable.Rows.Add(1, "Skip");
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ExcludedColumns: new HashSet<string>(new[] { "helper" }, StringComparer.OrdinalIgnoreCase))
            });

        Assert.Equal(new[] { "Id" }, destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public async Task CopyAsync_AppliesColumnTypeConversionsUsingSuppliedDictionaryComparer()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("ScoreText", typeof(string));
        sourceTable.Rows.Add("42");
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["scoretext"] = DbaTableCopyColumnType.Int32
                    })
            });

        Assert.Equal(typeof(int), destination.Rows.Columns["ScoreText"]!.DataType);
        Assert.Equal(42, destination.Rows.Rows[0]["ScoreText"]);
    }

    [Theory]
    [InlineData(0, null, null)]
    [InlineData(100, 0, null)]
    [InlineData(100, null, 0)]
    public async Task CopyAsync_RejectsInvalidOptions(int pageSize, int? batchSize, int? timeout)
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new DbaTableCopyOptions
            {
                PageSize = pageSize,
                BatchSize = batchSize,
                BulkCopyTimeout = timeout
            }));
    }

    [Fact]
    public async Task CopyAsync_RejectsInvalidDefinitions()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("", "DestinationRows") }));
    }

    [Fact]
    public async Task CopyAsync_RejectsDuplicateMappedDestinationColumns()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["FirstName"] = "Name",
                        ["LastName"] = "Name"
                    })
            }));

        Assert.Contains("destination column names cannot contain duplicates", exception.Message);
        Assert.False(destination.ClearCalled);
    }

    [Fact]
    public async Task CopyAsync_AllowsCaseDistinctMappedDestinationColumns()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("FirstName", typeof(string));
        sourceTable.Columns.Add("LastName", typeof(string));
        sourceTable.Rows.Add("Upper", "Lower");
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["FirstName"] = "Name",
                        ["LastName"] = "name"
                    })
            });

        Assert.Equal(new[] { "Name", "name" }, destination.Rows.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public async Task CopyAsync_RejectsMappedDestinationCollisionWithPassthroughColumn()
    {
        var sourceTable = new DataTable("SourceRows");
        sourceTable.Columns.Add("A", typeof(string));
        sourceTable.Columns.Add("B", typeof(string));
        sourceTable.Rows.Add("Mapped", "Passthrough");
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[]
            {
                new DbaTableCopyDefinition(
                    "SourceRows",
                    "DestinationRows",
                    ColumnMappings: new Dictionary<string, string>
                    {
                        ["A"] = "B"
                    })
            },
            new DbaTableCopyOptions { ClearDestination = true }));

        Assert.Contains("duplicate destination column 'B'", exception.Message);
        Assert.False(destination.ClearCalled);
    }

    private static void AssertProgress(DbaTableCopyProgress progress, long rowsCopied, long sourceRows, int pageRows)
    {
        Assert.Equal("Rows", progress.TableName);
        Assert.Equal(rowsCopied, progress.RowsCopied);
        Assert.Equal(sourceRows, progress.SourceRows);
        Assert.Equal(pageRows, progress.PageRows);
        Assert.Equal(rowsCopied * 100d / sourceRows, progress.PercentComplete);
    }

    private static DataTable CreateRows(int rows)
    {
        var table = new DataTable("SourceRows");
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));

        for (var i = 1; i <= rows; i++)
        {
            table.Rows.Add(i, $"Row {i}");
        }

        return table;
    }

    private sealed class MemoryTableCopySource : IDbaTableCopySource
    {
        private readonly DataTable _rows;

        public MemoryTableCopySource(DataTable rows)
            => _rows = rows;

        public List<long> RequestedOffsets { get; } = new();

        public List<int> RequestedPageSizes { get; } = new();

        public int CountCalls { get; private set; }

        public int ReadCalls { get; private set; }

        public bool ThrowOnCountRows { get; init; }

        public bool ThrowOnRead { get; init; }

        public long? SourceRowCountOverride { get; init; }

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        {
            CountCalls++;
            if (ThrowOnCountRows)
            {
                throw new InvalidOperationException("Count failed.");
            }

            return Task.FromResult<long?>(SourceRowCountOverride ?? _rows.Rows.Count);
        }

        public Task<DataTable> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
        {
            if (ThrowOnRead)
            {
                throw new InvalidOperationException("Read failed.");
            }

            ReadCalls++;
            RequestedOffsets.Add(request.Offset);
            RequestedPageSizes.Add(request.PageSize);

            var page = _rows.Clone();
            page.TableName = request.Definition.DestinationName;
            foreach (var row in _rows.AsEnumerable().Skip((int)request.Offset).Take(request.PageSize))
            {
                page.ImportRow(row);
            }

            return Task.FromResult(page);
        }
    }

    private sealed class MemoryTableCopyDestination : IDbaTableCopyDestination, IDbaTableCopyPagePreflightDestination, IDbaTableCopyEmptyPageDestination
    {
        private readonly long? _destinationRowCountOverride;

        public MemoryTableCopyDestination(long? destinationRowCountOverride = null)
            => _destinationRowCountOverride = destinationRowCountOverride;

        public bool ClearCalled { get; private set; }

        public List<string> ClearOrder { get; } = new();

        public List<string> WriteOrder { get; } = new();

        public DataTable Rows { get; } = new("DestinationRows");

        public string? ThrowOnCountDestinationName { get; init; }

        public bool ThrowOnCountBeforeWrite { get; init; }

        public int CountCalls { get; private set; }

        public bool ThrowOnValidatePage { get; init; }

        public int ValidatePageCalls { get; private set; }

        public bool WriteEmptyPages { get; init; }

        public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        {
            ClearCalled = true;
            ClearOrder.Add(definition.DestinationName);
            Rows.Rows.Clear();
            return Task.CompletedTask;
        }

        public Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
        {
            WriteOrder.Add(definition.DestinationName);
            if (Rows.Columns.Count == 0)
            {
                foreach (DataColumn column in page.Columns)
                {
                    Rows.Columns.Add(column.ColumnName, column.DataType);
                }
            }

            foreach (DataRow row in page.Rows)
            {
                Rows.ImportRow(row);
            }

            return Task.CompletedTask;
        }

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        {
            CountCalls++;
            if (string.Equals(definition.DestinationName, ThrowOnCountDestinationName, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Destination count failed.");
            }

            if (ThrowOnCountBeforeWrite && WriteOrder.Count == 0)
            {
                throw new InvalidOperationException("Destination count before write failed.");
            }

            return Task.FromResult<long?>(_destinationRowCountOverride ?? Rows.Rows.Count);
        }

        public void ValidatePage(DbaTableCopyDefinition definition, DataTable page)
        {
            ValidatePageCalls++;
            if (ThrowOnValidatePage)
            {
                throw new InvalidOperationException("Destination page preflight failed.");
            }
        }

        public bool ShouldWriteEmptyPage(DbaTableCopyDefinition definition)
            => WriteEmptyPages;
    }
}
