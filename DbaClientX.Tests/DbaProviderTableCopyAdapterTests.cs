using System.Data;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using DBAClientX;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using NpgsqlTypes;

namespace DbaClientX.Tests;

public class DbaProviderTableCopyAdapterBaseTests
{
    [Fact]
    public async Task PostgreSqlConsistentReadSession_RequiresDefinitionsForForeignTableValidation()
    {
        var source = new PostgreSqlTableCopyAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.PostgreSql,
            ConnectionString = "Host=localhost;Database=test;Username=test;Password=test;SslMode=Require",
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot
        });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            source.OpenReadSessionAsync());

        Assert.Contains("definitions", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsAssignableFrom<IDbaTableCopyDefinitionReadSession>(source);
    }

    [Fact]
    public void PostgreSqlConsistentReadSession_RejectsForeignRelations()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            PostgreSqlTableCopyAdapter.ValidateConsistentSourceRelationKind(
                "public.remote_rows",
                "f",
                containsForeignRelation: true,
                DbaTableCopyReadConsistency.Snapshot));

        Assert.Contains("foreign source table", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("stable remote snapshot", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgreSqlConsistentReadSession_RejectsPartitionTreesWithForeignDescendants()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            PostgreSqlTableCopyAdapter.ValidateConsistentSourceRelationKind(
                "public.partitioned_rows",
                "p",
                containsForeignRelation: true,
                DbaTableCopyReadConsistency.Serializable));

        Assert.Contains("partition tree", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("pg_catalog.pg_inherits", PostgreSqlTableCopyAdapter.PostgreSqlConsistentSourceRelationQuery, StringComparison.Ordinal);
        Assert.Contains("descendant.relkind = 'f'", PostgreSqlTableCopyAdapter.PostgreSqlConsistentSourceRelationQuery, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_rewrite", PostgreSqlTableCopyAdapter.PostgreSqlConsistentSourceRelationQuery, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_depend", PostgreSqlTableCopyAdapter.PostgreSqlConsistentSourceRelationQuery, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("point", "b", null, null, true)]
    [InlineData("int8range", "r", null, null, true)]
    [InlineData("int8multirange", "m", null, null, true)]
    [InlineData("_point", "b", "point", "b", true)]
    [InlineData("_int8range", "b", "int8range", "r", true)]
    [InlineData("_int4", "b", "int4", "b", true)]
    [InlineData("tsvector", "b", null, null, true)]
    [InlineData("custom_composite", "c", null, null, true)]
    [InlineData("custom_enum", "e", null, null, true)]
    [InlineData("_custom_enum", "b", "custom_enum", "e", true)]
    [InlineData("inet", "b", null, null, true)]
    [InlineData("cidr", "b", null, null, true)]
    [InlineData("macaddr", "b", null, null, true)]
    [InlineData("macaddr8", "b", null, null, true)]
    [InlineData("int8", "b", null, null, false)]
    public void PostgreSqlDestinationCompatibility_ClassifiesProviderSpecificTypes(
        string typeName,
        string typeKind,
        string? elementTypeName,
        string? elementTypeKind,
        bool expected)
    {
        Assert.Equal(
            expected,
            PostgreSqlTableCopyAdapter.IsProviderSpecificPostgreSqlType(
                typeName,
                typeKind,
                elementTypeName,
                elementTypeKind));
    }

    [Fact]
    public void PostgreSqlDestinationCompatibility_AllowsExplicitStringProjection()
    {
        var definition = new DbaTableCopyDefinition(
            "source_rows",
            "destination_rows",
            ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase)
            {
                ["shape"] = DbaTableCopyColumnType.String
            });

        Assert.True(PostgreSqlTableCopyAdapter.IsPortableProviderProjection(definition, "SHAPE"));
        Assert.False(PostgreSqlTableCopyAdapter.IsPortableProviderProjection(definition, "SHAPE", allowStringConversion: false));
        Assert.False(PostgreSqlTableCopyAdapter.IsPortableProviderProjection(definition, "period"));
        Assert.Contains("value_type.typtype", PostgreSqlTableCopyAdapter.PostgreSqlProviderSpecificColumnsQuery, StringComparison.Ordinal);
        Assert.Contains("element_type.typtype", PostgreSqlTableCopyAdapter.PostgreSqlProviderSpecificColumnsQuery, StringComparison.Ordinal);
        Assert.Contains("format_type(attribute.atttypid, attribute.atttypmod)", PostgreSqlTableCopyAdapter.PostgreSqlProviderSpecificColumnsQuery, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("e", null, true)]
    [InlineData("b", "e", true)]
    [InlineData("b", "b", false)]
    public void PostgreSqlDestinationCompatibility_ClassifiesUnmappedEnums(
        string typeKind,
        string? elementTypeKind,
        bool expected)
    {
        Assert.Equal(expected, PostgreSqlTableCopyAdapter.IsPostgreSqlEnum(typeKind, elementTypeKind));
    }

    [Theory]
    [InlineData("date", true)]
    [InlineData("timestamp", true)]
    [InlineData("timestamptz", true)]
    [InlineData("time", false)]
    [InlineData("interval", false)]
    public void PostgreSqlDestinationCompatibility_ClassifiesInfinityCapableTypes(string typeName, bool expected)
    {
        Assert.Equal(expected, PostgreSqlTableCopyAdapter.IsPostgreSqlInfinityCapableType(typeName));
    }

    [Fact]
    public async Task MySqlConsistentReadSession_RequiresDefinitionsForEngineValidation()
    {
        var source = new MySqlTableCopyAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.MySql,
            ConnectionString = "Server=localhost;Database=test;User ID=test;Password=test;",
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot
        });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            source.OpenReadSessionAsync());

        Assert.Contains("definitions", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsAssignableFrom<IDbaTableCopyDefinitionReadSession>(source);
    }

    [Fact]
    public void KeysetContinuationToken_RoundTripsUnsignedBigIntWithoutNarrowing()
    {
        var tokenType = typeof(DbaTableCopyDefinition).Assembly.GetType(
            "DBAClientX.DataMovement.DbaKeysetContinuationToken",
            throwOnError: true)!;
        var encode = tokenType.GetMethod("Encode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Encode");
        var decode = tokenType.GetMethod("Decode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Decode");
        var definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" })
        {
            UseKeysetPagination = true
        };
        using var table = new DataTable();
        table.Columns.Add("Id", typeof(ulong));
        DataRow row = table.Rows.Add(ulong.MaxValue);

        var token = Assert.IsType<string>(encode.Invoke(null, new object[] { definition, row }));
        var values = Assert.IsType<object[]>(decode.Invoke(null, new object?[] { definition, token }));

        Assert.Equal(ulong.MaxValue, Assert.IsType<ulong>(Assert.Single(values)));
    }

    [Fact]
    public void KeysetContinuationToken_RoundTripsPostgreSqlDateAndTimeValues()
    {
        var tokenType = typeof(DbaTableCopyDefinition).Assembly.GetType(
            "DBAClientX.DataMovement.DbaKeysetContinuationToken",
            throwOnError: true)!;
        var encode = tokenType.GetMethod("Encode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Encode");
        var decode = tokenType.GetMethod("Decode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Decode");
        var definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "EventDate", "EventTime" })
        {
            UseKeysetPagination = true
        };
        using var table = new DataTable();
        table.Columns.Add("EventDate", typeof(DateOnly));
        table.Columns.Add("EventTime", typeof(TimeOnly));
        var expectedDate = new DateOnly(2026, 9, 20);
        var expectedTime = new TimeOnly(23, 59, 58, 123).Add(TimeSpan.FromTicks(4567));
        DataRow row = table.Rows.Add(expectedDate, expectedTime);

        var token = Assert.IsType<string>(encode.Invoke(null, new object[] { definition, row }));
        var values = Assert.IsType<object[]>(decode.Invoke(null, new object?[] { definition, token }));

        Assert.Equal(expectedDate, Assert.IsType<DateOnly>(values[0]));
        Assert.Equal(expectedTime, Assert.IsType<TimeOnly>(values[1]));
    }

    [Fact]
    public void KeysetContinuationToken_RoundTripsYearMonthIntervals()
    {
        var tokenType = typeof(DbaTableCopyDefinition).Assembly.GetType(
            "DBAClientX.DataMovement.DbaKeysetContinuationToken",
            throwOnError: true)!;
        var encode = tokenType.GetMethod("Encode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Encode");
        var decode = tokenType.GetMethod("Decode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Decode");
        var definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Period" })
        {
            UseKeysetPagination = true
        };
        using var table = new DataTable();
        table.Columns.Add("Period", typeof(DbaYearMonthInterval));
        var expected = new DbaYearMonthInterval(-27);
        DataRow row = table.Rows.Add(expected);

        var token = Assert.IsType<string>(encode.Invoke(null, new object[] { definition, row }));
        var values = Assert.IsType<object[]>(decode.Invoke(null, new object?[] { definition, token }));

        Assert.Equal(expected, Assert.IsType<DbaYearMonthInterval>(Assert.Single(values)));
    }

    [Fact]
    public void KeysetContinuationToken_RoundTripsCalendarIntervals()
    {
        var tokenType = typeof(DbaTableCopyDefinition).Assembly.GetType(
            "DBAClientX.DataMovement.DbaKeysetContinuationToken",
            throwOnError: true)!;
        var encode = tokenType.GetMethod("Encode", BindingFlags.Static | BindingFlags.NonPublic)!;
        var decode = tokenType.GetMethod("Decode", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Period" })
        {
            UseKeysetPagination = true
        };
        using var table = new DataTable();
        table.Columns.Add("Period", typeof(DbaCalendarInterval));
        var expected = new DbaCalendarInterval(-14, 3, 4_500_001);
        DataRow row = table.Rows.Add(expected);

        string token = Assert.IsType<string>(encode.Invoke(null, new object[] { definition, row }));
        object[] values = Assert.IsType<object[]>(decode.Invoke(null, new object?[] { definition, token }));

        Assert.Equal(expected, Assert.IsType<DbaCalendarInterval>(Assert.Single(values)));
    }

    [Fact]
    public void KeysetContinuationToken_RoundTripsArbitraryDecimals()
    {
        Type tokenType = typeof(DbaTableCopyEngine).Assembly.GetType(
            "DBAClientX.DataMovement.DbaKeysetContinuationToken",
            throwOnError: true)!;
        MethodInfo encode = tokenType.GetMethod("Encode", BindingFlags.Static | BindingFlags.NonPublic)!;
        MethodInfo decode = tokenType.GetMethod("Decode", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definition = new DbaTableCopyDefinition("Source", "Destination", new[] { "Amount" })
        {
            UseKeysetPagination = true
        };
        using var table = new DataTable();
        table.Columns.Add("Amount", typeof(DbaArbitraryDecimal));
        var expected = new DbaArbitraryDecimal("1.25e30");
        table.Rows.Add(expected);

        string token = Assert.IsType<string>(encode.Invoke(null, new object[] { definition, table.Rows[0] }));
        object[] values = Assert.IsType<object[]>(decode.Invoke(null, new object?[] { definition, token }));

        Assert.Equal(expected, Assert.IsType<DbaArbitraryDecimal>(Assert.Single(values)));
    }

    [Fact]
    public void KeysetContinuationToken_RoundTripsNetworkValues()
    {
        var tokenType = typeof(DbaTableCopyDefinition).Assembly.GetType(
            "DBAClientX.DataMovement.DbaKeysetContinuationToken",
            throwOnError: true)!;
        var encode = tokenType.GetMethod("Encode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Encode");
        var decode = tokenType.GetMethod("Decode", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(tokenType.FullName, "Decode");
        var definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Address", "Network", "Mac" })
        {
            UseKeysetPagination = true
        };
        using var table = new DataTable();
        table.Columns.Add("Address", typeof(IPAddress));
        table.Columns.Add("Network", typeof(DbaIpNetwork));
        table.Columns.Add("Mac", typeof(PhysicalAddress));
        var expectedAddress = IPAddress.Parse("2001:db8::42");
        var expectedNetwork = new DbaIpNetwork(IPAddress.Parse("198.51.100.0"), 24);
        var expectedMac = PhysicalAddress.Parse("001122AABBCC");
        DataRow row = table.Rows.Add(expectedAddress, expectedNetwork, expectedMac);

        var token = Assert.IsType<string>(encode.Invoke(null, new object[] { definition, row }));
        var values = Assert.IsType<object[]>(decode.Invoke(null, new object?[] { definition, token }));

        Assert.Equal(expectedAddress, Assert.IsType<IPAddress>(values[0]));
        Assert.Equal(expectedNetwork, Assert.IsType<DbaIpNetwork>(values[1]));
        Assert.Equal(expectedMac, Assert.IsType<PhysicalAddress>(values[2]));
    }

    [Fact]
    public void ContentHasher_AcceptsYearMonthIntervals()
    {
        using var first = new DataTable();
        first.Columns.Add("Period", typeof(DbaYearMonthInterval));
        first.Rows.Add(new DbaYearMonthInterval(27));

        using var second = first.Copy();

        Assert.Equal(ComputeContentHash(first, "Period"), ComputeContentHash(second, "Period"));
    }

    [Fact]
    public void ContentHasher_PreservesCalendarIntervalComponents()
    {
        using var first = new DataTable();
        first.Columns.Add("Period", typeof(DbaCalendarInterval));
        first.Rows.Add(new DbaCalendarInterval(1, 2, 3));
        using var same = first.Copy();
        using var changed = first.Clone();
        changed.Rows.Add(new DbaCalendarInterval(1, 3, 3));

        Assert.Equal(ComputeContentHash(first, "Period"), ComputeContentHash(same, "Period"));
        Assert.NotEqual(ComputeContentHash(first, "Period"), ComputeContentHash(changed, "Period"));
    }

    [Theory]
    [InlineData("0012.5000", "12.5")]
    [InlineData("1.25e30", "1250000000000000000000000000000")]
    [InlineData("-0.000", "0")]
    public void ArbitraryDecimal_UsesStableProviderNeutralRepresentation(string input, string expected)
    {
        var value = new DbaArbitraryDecimal(input);

        Assert.Equal(expected, value.CanonicalValue);
        Assert.Equal(value, new DbaArbitraryDecimal(expected));
    }

    [Fact]
    public void ContentHasher_NormalizesArbitraryAndClrDecimals()
    {
        using var arbitrary = new DataTable();
        arbitrary.Columns.Add("Amount", typeof(DbaArbitraryDecimal));
        arbitrary.Rows.Add(new DbaArbitraryDecimal("12.500"));
        using var clr = new DataTable();
        clr.Columns.Add("Amount", typeof(decimal));
        clr.Rows.Add(12.5m);

        Assert.Equal(ComputeContentHash(clr, "Amount"), ComputeContentHash(arbitrary, "Amount"));
    }

    [Fact]
    public void ContentHasher_AcceptsNetworkValues()
    {
        using var first = new DataTable();
        first.Columns.Add("Address", typeof(IPAddress));
        first.Columns.Add("Network", typeof(DbaIpNetwork));
        first.Columns.Add("Mac", typeof(PhysicalAddress));
        first.Rows.Add(
            IPAddress.Parse("192.0.2.42"),
            new DbaIpNetwork(IPAddress.Parse("198.51.100.0"), 24),
            PhysicalAddress.Parse("001122AABBCC"));

        using var second = first.Copy();

        Assert.Equal(
            ComputeContentHash(first, "Address", "Network", "Mac"),
            ComputeContentHash(second, "Address", "Network", "Mac"));
    }

    [Fact]
    public void ContentHasher_AcceptsPostgreSqlArrayValues()
    {
        using var first = new DataTable();
        first.Columns.Add("Values", typeof(int[]));
        first.Rows.Add(new int[] { 1, 2, 3 });

        using var same = first.Copy();
        using var changed = first.Clone();
        changed.Rows.Add(new int[] { 1, 2, 4 });

        Assert.Equal(ComputeContentHash(first, "Values"), ComputeContentHash(same, "Values"));
        Assert.NotEqual(ComputeContentHash(first, "Values"), ComputeContentHash(changed, "Values"));
    }

    [Fact]
    public void ContentHasher_NormalizesPostgreSqlRangesAndMultiranges()
    {
        var normalizer = new PostgreSqlTableCopyAdapter(
            "Host=localhost;Database=db;Username=u;Password=p;SslMode=Require");
        var firstRange = new NpgsqlRange<int>(1, lowerBoundIsInclusive: true, 5, upperBoundIsInclusive: false);
        var changedRange = new NpgsqlRange<int>(1, lowerBoundIsInclusive: true, 6, upperBoundIsInclusive: false);
        using var first = new DataTable();
        first.Columns.Add("Range", typeof(NpgsqlRange<int>));
        first.Columns.Add("Multirange", typeof(NpgsqlRange<int>[]));
        first.Rows.Add(firstRange, new[] { firstRange, NpgsqlRange<int>.Empty });

        using var same = first.Copy();
        using var changed = first.Clone();
        changed.Rows.Add(changedRange, new[] { firstRange, NpgsqlRange<int>.Empty });

        Assert.Equal(
            ComputeContentHash(first, normalizer, "Range", "Multirange"),
            ComputeContentHash(same, normalizer, "Range", "Multirange"));
        Assert.NotEqual(
            ComputeContentHash(first, normalizer, "Range", "Multirange"),
            ComputeContentHash(changed, normalizer, "Range", "Multirange"));
    }

    [Fact]
    public void ContentHasher_NormalizesPostgreSqlGeometricValues()
    {
        var normalizer = new PostgreSqlTableCopyAdapter(
            "Host=localhost;Database=db;Username=u;Password=p;SslMode=Require");
        var points = new[] { new NpgsqlPoint(1, 2), new NpgsqlPoint(3, 4) };
        using var first = new DataTable();
        first.Columns.Add("Point", typeof(NpgsqlPoint));
        first.Columns.Add("Line", typeof(NpgsqlLine));
        first.Columns.Add("Segment", typeof(NpgsqlLSeg));
        first.Columns.Add("Box", typeof(NpgsqlBox));
        first.Columns.Add("Path", typeof(NpgsqlPath));
        first.Columns.Add("Polygon", typeof(NpgsqlPolygon));
        first.Columns.Add("Circle", typeof(NpgsqlCircle));
        first.Rows.Add(
            points[0],
            new NpgsqlLine(1, 2, 3),
            new NpgsqlLSeg(points[0], points[1]),
            new NpgsqlBox(points[1], points[0]),
            new NpgsqlPath(points, open: true),
            new NpgsqlPolygon(points),
            new NpgsqlCircle(points[0], 5));

        using var same = first.Copy();
        using var changed = first.Copy();
        changed.Rows[0]["Circle"] = new NpgsqlCircle(points[0], 6);
        string[] columns = { "Point", "Line", "Segment", "Box", "Path", "Polygon", "Circle" };

        Assert.Equal(
            ComputeContentHash(first, normalizer, columns),
            ComputeContentHash(same, normalizer, columns));
        Assert.NotEqual(
            ComputeContentHash(first, normalizer, columns),
            ComputeContentHash(changed, normalizer, columns));
    }

    [Theory]
    [InlineData("AllowZeroDateTime=true", "AllowZeroDateTime")]
    [InlineData("ConvertZeroDateTime=true", "ConvertZeroDateTime")]
    public void MySqlTableCopy_RejectsZeroDateProviderValuesBeforeReading(
        string option,
        string expectedOption)
    {
        var exception = Assert.Throws<ArgumentException>(() => new MySqlTableCopyAdapter(
            $"Server=localhost;Database=test;User ID=test;Password=test;{option}"));

        Assert.Contains(expectedOption, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MySqlTableIdentity_IsCollisionFreeForDelimiterCharacters()
    {
        string first = MySqlTableCopyAdapter.CreateTableIdentity("a", "b:c");
        string second = MySqlTableCopyAdapter.CreateTableIdentity("a:b", "c");

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void MySqlArbitraryDecimal_RehydratesBulkAndKeysetValuesLosslessly()
    {
        var number = new DbaArbitraryDecimal("1000000000000000000000000000000");
        using var page = new DataTable();
        page.Columns.Add("Amount", typeof(object));
        page.Rows.Add(12.5m);
        page.Rows.Add(number);

        using DataTable normalized = Assert.IsType<DataTable>(MySqlTableCopyAdapter.NormalizeBulkPage(page));
        Assert.Equal(12.5m, Assert.IsType<decimal>(normalized.Rows[0][0]));
        Assert.Equal(number.CanonicalValue, Assert.IsType<string>(normalized.Rows[1][0]));
        Assert.Equal(typeof(object), MySqlTableCopyAdapter.GetNormalizedFieldType(typeof(decimal), "DECIMAL"));

        using var command = new MySqlCommand();
        MySqlTableCopyAdapter.AddPageParameter(command, "@amount", number);
        MySqlParameter parameter = Assert.Single(command.Parameters.Cast<MySqlParameter>());
        Assert.Equal(MySqlDbType.NewDecimal, parameter.MySqlDbType);
        Assert.Equal(number.CanonicalValue, parameter.Value);
    }

    [Fact]
    public void MySqlArbitraryDecimal_CrossProviderCompatibilityRequiresExclusionOrStringConversion()
    {
        var direct = new DbaTableCopyDefinition("Source", "Destination");
        var excluded = direct with
        {
            ExcludedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Amount" }
        };
        var converted = direct with
        {
            ColumnMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["Amount"] = "Total" },
            ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase)
            {
                ["Total"] = DbaTableCopyColumnType.String
            }
        };

        Assert.False(MySqlTableCopyAdapter.IsPortableDecimalProjection(direct, "Amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableDecimalProjection(excluded, "amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableDecimalProjection(converted, "amount"));
    }

    [Fact]
    public void MySqlArbitraryDecimalProjection_HonorsConfiguredComparers()
    {
        var ordinalMapping = new DbaTableCopyDefinition(
            "Source",
            "Destination",
            ColumnMappings: new Dictionary<string, string>(StringComparer.Ordinal) { ["amount"] = "Total" },
            ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType>(StringComparer.Ordinal)
            {
                ["Total"] = DbaTableCopyColumnType.String
            });
        var ignoreCaseMapping = ordinalMapping with
        {
            ColumnMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["amount"] = "Total" }
        };
        var ordinalExclusion = new DbaTableCopyDefinition(
            "Source",
            "Destination",
            ExcludedColumns: new HashSet<string>(StringComparer.Ordinal) { "amount" });
        var ignoreCaseExclusion = ordinalExclusion with
        {
            ExcludedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "amount" }
        };
        var ordinalConversion = new DbaTableCopyDefinition(
            "Source",
            "Destination",
            ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType>(StringComparer.Ordinal)
            {
                ["amount"] = DbaTableCopyColumnType.String
            });
        var ignoreCaseConversion = ordinalConversion with
        {
            ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>(StringComparer.OrdinalIgnoreCase)
            {
                ["amount"] = DbaTableCopyColumnType.String
            }
        };

        Assert.False(MySqlTableCopyAdapter.IsPortableDecimalProjection(ordinalMapping, "Amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableDecimalProjection(ignoreCaseMapping, "Amount"));
        Assert.False(MySqlTableCopyAdapter.IsPortableDecimalProjection(ordinalExclusion, "Amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableDecimalProjection(ignoreCaseExclusion, "Amount"));
        Assert.False(MySqlTableCopyAdapter.IsPortableDecimalProjection(ordinalConversion, "Amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableDecimalProjection(ignoreCaseConversion, "Amount"));
    }

    [Fact]
    public void MySqlUnsignedProjection_RequiresDecimalOrStringConversion()
    {
        var direct = new DbaTableCopyDefinition("Source", "Destination");
        var decimalConversion = direct with
        {
            ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>
            {
                ["Amount"] = DbaTableCopyColumnType.Decimal
            }
        };
        var stringConversion = direct with
        {
            ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>
            {
                ["Amount"] = DbaTableCopyColumnType.String
            }
        };

        Assert.False(MySqlTableCopyAdapter.IsPortableUnsignedProjection(direct, "Amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableUnsignedProjection(decimalConversion, "Amount"));
        Assert.True(MySqlTableCopyAdapter.IsPortableUnsignedProjection(stringConversion, "Amount"));
        Assert.Contains("COLUMN_TYPE LIKE '%unsigned%'", MySqlTableCopyAdapter.MySqlTableCopyNumericColumnsQuery, StringComparison.Ordinal);
        Assert.Contains("DATA_TYPE = 'bit'", MySqlTableCopyAdapter.MySqlTableCopyNumericColumnsQuery, StringComparison.Ordinal);
        Assert.Contains("NUMERIC_PRECISION > 63", MySqlTableCopyAdapter.MySqlTableCopyNumericColumnsQuery, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(28, 0)]
    [InlineData(28, 28)]
    [InlineData(2, -3)]
    public void PostgreSqlNumericShape_AcceptsDecimalSafeShapes(int precision, int scale)
    {
        PostgreSqlTableCopyAdapter.ValidateNumericShape("Amount", precision, scale);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(29, 0)]
    [InlineData(28, -1)]
    [InlineData(29, 29)]
    public void PostgreSqlNumericShape_RejectsPotentiallyOversizedValues(int? precision, int? scale)
    {
        var exception = Assert.Throws<NotSupportedException>(() =>
            PostgreSqlTableCopyAdapter.ValidateNumericShape("Amount", precision, scale));

        Assert.Contains("System.Decimal precision", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Project it to text", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("numeric(28,0)[]")]
    [InlineData("numeric(2,-3)[]")]
    [InlineData("decimal(28,28)[]")]
    public void PostgreSqlNumericArrayShape_AcceptsDecimalSafeElements(string formattedType)
    {
        PostgreSqlTableCopyAdapter.ValidateNumericArrayShape("Amounts", formattedType);
    }

    [Theory]
    [InlineData("numeric[]")]
    [InlineData("numeric(100)[]")]
    [InlineData("numeric(28,-1)[]")]
    public void PostgreSqlNumericArrayShape_RejectsPotentiallyOversizedElements(string formattedType)
    {
        var exception = Assert.Throws<NotSupportedException>(() =>
            PostgreSqlTableCopyAdapter.ValidateNumericArrayShape("Amounts", formattedType));

        Assert.Contains("System.Decimal precision", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PostgreSqlSchemaPreflight_RejectsOmittedSequenceDefaults()
    {
        var columns = new[]
        {
            new DbaColumnInfo("public", "rows", "id", "integer")
            {
                DefaultExpression = "nextval('rows_id_seq'::regclass)"
            },
            new DbaColumnInfo("public", "rows", "value", "text")
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            PostgreSqlTableCopyAdapter.ValidateRollbackSafeGeneratorProjection(
                "public.rows",
                new[] { "value" },
                columns));

        Assert.Contains("sequence advances are not rolled back", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public void SqlServerSchemaPreflight_RequiresProjectedIdentityValues(
        bool keepIdentity,
        bool projectIdentity,
        bool shouldReject)
    {
        var columns = new[]
        {
            new DbaColumnInfo("dbo", "Rows", "Id", "bigint") { IsIdentity = true },
            new DbaColumnInfo("dbo", "Rows", "Value", "nvarchar(50)")
        };
        string[] projected = projectIdentity ? new[] { "Id", "Value" } : new[] { "Value" };

        Exception? exception = Record.Exception(() =>
            SqlServerTableCopyAdapter.ValidateRollbackSafeGeneratorProjection(
                "dbo.Rows",
                projected,
                columns,
                keepIdentity));

        if (shouldReject)
        {
            var invalid = Assert.IsType<InvalidOperationException>(exception);
            Assert.Contains("identity", invalid.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(keepIdentity ? "not rolled back" : "KeepIdentity", invalid.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Null(exception);
        }
    }

    [Fact]
    public void MySqlSchemaPreflight_RejectsOmittedAutoIncrementColumns()
    {
        var columns = new[]
        {
            new DbaColumnInfo("app", "rows", "Id", "bigint unsigned") { IsIdentity = true },
            new DbaColumnInfo("app", "rows", "Value", "varchar(50)")
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            MySqlTableCopyAdapter.ValidateRollbackSafeGeneratorProjection(
                "app.rows",
                new[] { "Value" },
                columns));

        Assert.Contains("auto-increment advances are not rolled back", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(null, false, true)]
    [InlineData(0L, false, true)]
    [InlineData(0L, true, false)]
    [InlineData(42L, false, false)]
    public void MySqlSchemaPreflight_RejectsValuesThatInvokeAutoIncrement(
        long? value,
        bool noAutoValueOnZero,
        bool shouldReject)
    {
        using var page = new DataTable();
        page.Columns.Add("Id", typeof(long));
        page.Rows.Add(value.HasValue ? value.Value : DBNull.Value);
        var columns = new[]
        {
            new DbaColumnInfo("app", "rows", "Id", "bigint") { IsIdentity = true }
        };

        Exception? exception = Record.Exception(() =>
            MySqlTableCopyAdapter.ValidateRollbackSafeGeneratorValues(
                "app.rows",
                page,
                columns,
                noAutoValueOnZero));

        if (shouldReject)
        {
            var invalid = Assert.IsType<InvalidOperationException>(exception);
            Assert.Contains("auto-increment advances are not rolled back", invalid.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Null(exception);
        }
    }

    [Fact]
    public void MySqlCheckpointStorage_RequiresSelectedDatabase()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            MySqlTableCopyAdapter.ValidateCheckpointDatabase(string.Empty));

        Assert.Contains("selected database", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RollbackOnlyPreflight_RejectsProviderTriggersThatCanEscapeRollback()
    {
        string sqlServer = SqlServerTableCopyAdapter.SqlServerRollbackUnsafeTriggerQuery;
        Assert.Contains("sys.triggers", sqlServer, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ExecIsInsertTrigger", sqlServer, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ExecIsDeleteTrigger", sqlServer, StringComparison.OrdinalIgnoreCase);

        string postgreSql = PostgreSqlTableCopyAdapter.PostgreSqlRollbackUnsafeTriggerQuery;
        Assert.Contains("pg_trigger", postgreSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NOT tgisinternal", postgreSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("tgtype & 4", postgreSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("tgtype & 8", postgreSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("pg_catalog.pg_inherits", postgreSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("relation_tree", postgreSql, StringComparison.OrdinalIgnoreCase);

        string mySql = MySqlTableCopyAdapter.MySqlRollbackUnsafeTriggerQuery;
        Assert.Contains("INFORMATION_SCHEMA.TRIGGERS", mySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EVENT_MANIPULATION IN ('INSERT', 'DELETE')", mySql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("BINARY EVENT_OBJECT_TABLE = BINARY @table", mySql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql, "\"UserId\"", "UserId")]
    [InlineData(DbaTableCopyProvider.Oracle, "\"User\"", "User")]
    public void KeysetResultColumns_ResolveDelimitedIdentifiers(
        DbaTableCopyProvider provider,
        string orderedColumn,
        string resultColumn)
    {
        using var table = new DataTable();
        table.Columns.Add(resultColumn, typeof(long));
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod(
            "ResolveKeysetResultColumns",
            BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "ResolveKeysetResultColumns");

        var resolved = Assert.IsAssignableFrom<IReadOnlyList<string>>(method.Invoke(
            null,
            new object[] { provider, table.Columns, new[] { orderedColumn }, "SourceRows" }));

        Assert.Equal(resultColumn, Assert.Single(resolved));
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql, "ID", "ID", "id", "id")]
    [InlineData(DbaTableCopyProvider.Oracle, "id", "id", "ID", "ID")]
    public void KeysetResultColumns_ApplyProviderFoldingBeforeExactLookup(
        DbaTableCopyProvider provider,
        string orderedColumn,
        string firstResultColumn,
        string secondResultColumn,
        string expected)
    {
        using var table = new DataTable();
        table.CaseSensitive = true;
        table.Columns.Add(firstResultColumn, typeof(long));
        table.Columns.Add(secondResultColumn, typeof(long));
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod(
            "ResolveKeysetResultColumns",
            BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "ResolveKeysetResultColumns");

        var resolved = Assert.IsAssignableFrom<IReadOnlyList<string>>(method.Invoke(
            null,
            new object[] { provider, table.Columns, new[] { orderedColumn }, "SourceRows" }));

        Assert.Equal(expected, Assert.Single(resolved));
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.PostgreSql, "event-id")]
    [InlineData(DbaTableCopyProvider.PostgreSql, "select")]
    [InlineData(DbaTableCopyProvider.Oracle, "event-id")]
    [InlineData(DbaTableCopyProvider.Oracle, "select")]
    public void KeysetResultColumns_PreserveAutomaticallyDelimitedPhysicalSpelling(
        DbaTableCopyProvider provider,
        string columnName)
    {
        using var table = new DataTable { CaseSensitive = true };
        table.Columns.Add(columnName, typeof(long));
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod(
            "ResolveKeysetResultColumns",
            BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "ResolveKeysetResultColumns");

        var resolved = Assert.IsAssignableFrom<IReadOnlyList<string>>(method.Invoke(
            null,
            new object[] { provider, table.Columns, new[] { columnName }, "SourceRows" }));

        Assert.Equal(columnName, Assert.Single(resolved));
    }

    [Fact]
    public void ContentHasher_NormalizesPostgreSqlDateAndTimeRepresentations()
    {
        using var postgreSqlTable = new DataTable();
        postgreSqlTable.Columns.Add("EventDate", typeof(DateOnly));
        postgreSqlTable.Columns.Add("EventTime", typeof(TimeOnly));
        postgreSqlTable.Rows.Add(new DateOnly(2026, 9, 20), new TimeOnly(12, 34, 56).Add(TimeSpan.FromTicks(7890)));

        using var conventionalTable = new DataTable();
        conventionalTable.Columns.Add("EventDate", typeof(DateTime));
        conventionalTable.Columns.Add("EventTime", typeof(TimeSpan));
        conventionalTable.Rows.Add(new DateTime(2026, 9, 20), new TimeSpan(0, 12, 34, 56).Add(TimeSpan.FromTicks(7890)));

        Assert.Equal(
            ComputeContentHash(conventionalTable, "EventDate", "EventTime"),
            ComputeContentHash(postgreSqlTable, "EventDate", "EventTime"));
    }

    [Fact]
    public void ContentHasher_NormalizesProviderGuidAndUtcTimestampRepresentations()
    {
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        using var source = new DataTable();
        source.Columns.Add("Identifier", typeof(Guid));
        source.Columns.Add("Instant", typeof(DateTimeOffset));
        source.Rows.Add(guid, new DateTimeOffset(2026, 9, 20, 10, 0, 0, TimeSpan.Zero));

        using var destination = new DataTable();
        destination.Columns.Add("Identifier", typeof(byte[]));
        destination.Columns.Add("Instant", typeof(DateTime));
        destination.Columns["Instant"]!.DateTimeMode = DataSetDateTime.Utc;
        destination.Rows.Add(guid.ToByteArray(), new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Utc));

        Assert.Equal(
            ComputeContentHash(source, "Identifier", "Instant"),
            ComputeContentHash(destination, "Identifier", "Instant"));
    }

    [Fact]
    public void ContentHasher_DistinguishesDateTimeOffsetSourceOffsets()
    {
        using var utc = new DataTable();
        utc.Columns.Add("Instant", typeof(DateTimeOffset));
        utc.Rows.Add(new DateTimeOffset(2026, 9, 21, 10, 0, 0, TimeSpan.Zero));
        using var offset = utc.Clone();
        offset.Rows.Add(new DateTimeOffset(2026, 9, 21, 12, 0, 0, TimeSpan.FromHours(2)));

        Assert.NotEqual(ComputeContentHash(utc, "Instant"), ComputeContentHash(offset, "Instant"));
    }

    [Fact]
    public async Task SQLiteSnapshotReadSession_ExcludesRowsCommittedAfterFirstPage()
    {
        var sourcePath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "PRAGMA journal_mode=WAL;");
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, Payload TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO SourceRows VALUES (1, 'One'), (2, 'Two');");
            }

            var source = new SQLiteTableCopyAdapter(new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SQLite,
                ConnectionString = sourcePath,
                DefaultOrderByColumns = new[] { "Id" },
                ReadConsistency = DbaTableCopyReadConsistency.Snapshot
            });
            var definition = new DbaTableCopyDefinition(
                "SourceRows",
                "DestinationRows",
                new[] { "Id" })
            {
                UseKeysetPagination = true
            };

            using var session = await source.OpenReadSessionAsync();
            using var first = await source.ReadPageAsync(new DbaTableCopyPageRequest(definition, null, 1));

            await using (var writer = new SqliteConnection(SQLite.BuildConnectionString(sourcePath)))
            {
                await writer.OpenAsync();
                await using var command = writer.CreateCommand();
                command.CommandText = "INSERT INTO SourceRows VALUES (3, 'Three');";
                await command.ExecuteNonQueryAsync();
            }

            using var second = await source.ReadPageAsync(new DbaTableCopyPageRequest(definition, first.ContinuationToken, 1));
            using var third = await source.ReadPageAsync(new DbaTableCopyPageRequest(definition, second.ContinuationToken, 1));

            Assert.Equal(1L, first.Data.Rows[0].Field<long>("Id"));
            Assert.Equal(2L, second.Data.Rows[0].Field<long>("Id"));
            Assert.Empty(third.Data.Rows.Cast<DataRow>());
        }
        finally
        {
            DeleteIfExists(sourcePath);
        }
    }

    [Fact]
    public async Task CopyAsync_CopiesRowsBetweenSQLiteConnectionStrings()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One'), (2, 'Two'), (3, 'Three');");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "Id" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) },
                new DbaTableCopyOptions { PageSize = 2 });

            Assert.True(result.Verified);
            Assert.Equal(3, result.CopiedRows);
            Assert.Equal(3, result.SourceRows);
            Assert.Equal(3, result.DestinationRows);

            using (var sqlite = new SQLite())
            {
                var count = sqlite.ExecuteScalar(destinationPath, "SELECT COUNT(*) FROM DestinationRows;");
                Assert.Equal(3L, Convert.ToInt64(count));
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_CopiesRowsBetweenSQLiteRawPathsContainingEqualsSigns()
    {
        var sourcePath = CreateTempDatabasePath("source=blue");
        var destinationPath = CreateTempDatabasePath("destination=green");
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One'), (2, 'Two');");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                sourcePath,
                new[] { "Id" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_CopiesRowsFromSQLiteFullUriConnectionString()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE SourceRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE DestinationRows (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO SourceRows (Id, DisplayName) VALUES (1, 'One'), (2, 'Two');");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "FullUri=" + new Uri(sourcePath).AbsoluteUri,
                new[] { "Id" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }) });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public void SQLiteTableCopyAdapter_PreservesExplicitPoolingOption()
    {
        var adapter = new SQLiteTableCopyAdapter("Data Source=:memory:;Mode=Memory;Cache=Shared;Pooling=True");

        var connectionString = InvokeResolveSQLiteConnectionString(adapter);
        var builder = new SqliteConnectionStringBuilder(connectionString);

        Assert.True(builder.Pooling);
    }

    [Fact]
    public void SQLiteTableCopyAdapter_DefaultsPoolingOffWhenOptionIsAbsent()
    {
        var adapter = new SQLiteTableCopyAdapter("Data Source=:memory:;Mode=Memory;Cache=Shared");

        var connectionString = InvokeResolveSQLiteConnectionString(adapter);
        var builder = new SqliteConnectionStringBuilder(connectionString);

        Assert.False(builder.Pooling);
    }

    [Fact]
    public async Task CopyAsync_SQLiteBulkWritePreservesDotsInsideQuotedDestinationSegments()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE \"Rows.Source\" (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE \"Rows.Current\" (Id INTEGER NOT NULL PRIMARY KEY, DisplayName TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO \"Rows.Source\" (Id, DisplayName) VALUES (1, 'One'), (2, 'Two');");
                sqlite.ExecuteNonQuery(destinationPath, "INSERT INTO \"Rows.Current\" (Id, DisplayName) VALUES (99, 'Old');");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "Id" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("\"Rows.Source\"", "\"Rows.Current\"", new[] { "Id" }) },
                new DbaTableCopyOptions
                {
                    ClearDestination = true,
                    PageSize = 1
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.CopiedRows);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var rows = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT Id, DisplayName FROM \"Rows.Current\" ORDER BY Id;"));
                Assert.Equal(2, rows.Rows.Count);
                Assert.Equal("One", rows.Rows[0]["DisplayName"]);
                Assert.Equal("Two", rows.Rows[1]["DisplayName"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_DeduplicatesSQLiteSourceRowsByCaseInsensitiveKey()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, StatusId INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE ProbeIndex (ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, StatusId INTEGER NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeIndex (ProbeName, LastCompletedUtcMs, StatusId) VALUES ('Server1', 10, 1), ('server1', 20, 2), ('Server2', 15, 3);");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "ProbeName" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[]
                {
                    new DbaTableCopyDefinition(
                        "ProbeIndex",
                        "ProbeIndex",
                        new[] { "ProbeName" },
                        SourceOptions: new DbaTableCopySourceOptions(
                            new[] { "ProbeName" },
                            new[] { "LastCompletedUtcMs" },
                            DeduplicateCaseInsensitive: true))
                },
                new DbaTableCopyOptions { PageSize = 1 });

            Assert.True(result.Verified);
            Assert.Equal(2, result.SourceRows);
            Assert.Equal(2, result.CopiedRows);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var rows = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT ProbeName, LastCompletedUtcMs, StatusId FROM ProbeIndex ORDER BY lower(ProbeName);"));
                Assert.Equal(2, rows.Rows.Count);
                Assert.Equal("server1", rows.Rows[0]["ProbeName"]);
                Assert.Equal(20L, Convert.ToInt64(rows.Rows[0]["LastCompletedUtcMs"]));
                Assert.Equal(2L, Convert.ToInt64(rows.Rows[0]["StatusId"]));
                Assert.Equal("Server2", rows.Rows[1]["ProbeName"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_PreservesSyntheticRankNamedSourceColumnWithoutDeduplication()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE ProbeIndex (Id INTEGER NOT NULL, __DbaXCRank_62D977CD TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE ProbeIndex (Id INTEGER NOT NULL, __DbaXCRank_62D977CD TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeIndex (Id, __DbaXCRank_62D977CD) VALUES (1, 'real-rank');");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "Id" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[]
                {
                    new DbaTableCopyDefinition("ProbeIndex", "ProbeIndex", new[] { "Id" })
                },
                new DbaTableCopyOptions { PageSize = 1 });

            Assert.True(result.Verified);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var rows = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT Id, __DbaXCRank_62D977CD FROM ProbeIndex ORDER BY Id;"));
                Assert.Equal(1, rows.Rows.Count);
                Assert.Equal("real-rank", rows.Rows[0]["__DbaXCRank_62D977CD"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_TreatsMissingSQLiteSourceTableAsEmptyWhenConfigured()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(sourcePath, "CREATE TABLE ExistingRows (Id INTEGER NOT NULL PRIMARY KEY);");
                sqlite.ExecuteNonQuery(destinationPath, "CREATE TABLE MissingRows (Id INTEGER NOT NULL PRIMARY KEY);");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                treatMissingTablesAsEmpty: true);
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath,
                treatMissingTablesAsEmpty: true);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[] { new DbaTableCopyDefinition("MissingRows", "MissingRows", new[] { "Id" }) });

            Assert.True(result.Verified);
            Assert.Equal(0, result.SourceRows);
            Assert.Equal(0, result.CopiedRows);
            Assert.Equal(0, result.DestinationRows);
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public async Task CopyAsync_ClearsDependentSQLiteTablesInReverseDefinitionOrder()
    {
        var sourcePath = CreateTempDatabasePath();
        var destinationPath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                CreateHistoryTables(sqlite, sourcePath);
                CreateHistoryTables(sqlite, destinationPath);
                sqlite.ExecuteNonQuery(
                    destinationPath,
                    """
                    CREATE TRIGGER BlockProbeResultDeleteBeforeMetadata
                    BEFORE DELETE ON ProbeResults
                    WHEN EXISTS (SELECT 1 FROM ProbeResultMetadata WHERE ResultId = OLD.ResultId)
                    BEGIN
                        SELECT RAISE(ABORT, 'metadata exists');
                    END;
                    """);
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeResults (ResultId, ProbeName, IsMaintenance) VALUES (1, 'Dns', 1);");
                sqlite.ExecuteNonQuery(sourcePath, "INSERT INTO ProbeResultMetadata (ResultId, MetaKey, MetaValue) VALUES (1, 'Zone', 'contoso.com');");
                sqlite.ExecuteNonQuery(destinationPath, "INSERT INTO ProbeResults (ResultId, ProbeName, IsMaintenance) VALUES (99, 'Old', 0);");
                sqlite.ExecuteNonQuery(destinationPath, "INSERT INTO ProbeResultMetadata (ResultId, MetaKey, MetaValue) VALUES (99, 'OldKey', 'OldValue');");
            }

            var source = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "ResultId" });
            var destination = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + destinationPath);

            var result = await new DbaTableCopyEngine().CopyAsync(
                source,
                destination,
                new[]
                {
                    new DbaTableCopyDefinition("ProbeResults", "ProbeResults", new[] { "ResultId" }),
                    new DbaTableCopyDefinition("ProbeResultMetadata", "ProbeResultMetadata", new[] { "ResultId", "MetaKey" })
                },
                new DbaTableCopyOptions
                {
                    ClearDestination = true,
                    PageSize = 1
                });

            Assert.True(result.Verified);
            Assert.Equal(2, result.SourceRows);
            Assert.Equal(2, result.CopiedRows);
            Assert.Equal(2, result.DestinationRows);
            using (var sqlite = new SQLite { ReturnType = ReturnType.DataTable })
            {
                var metadata = Assert.IsType<DataTable>(sqlite.Query(destinationPath, "SELECT ResultId, MetaKey, MetaValue FROM ProbeResultMetadata;"));
                var row = Assert.Single(metadata.Rows.Cast<DataRow>());
                Assert.Equal(1L, Convert.ToInt64(row["ResultId"]));
                Assert.Equal("Zone", row["MetaKey"]);
            }
        }
        finally
        {
            DeleteIfExists(sourcePath);
            DeleteIfExists(destinationPath);
        }
    }

    [Fact]
    public void BuildPageQuery_OracleFoldsSimpleIdentifiersToUppercase()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "Id" });

        var query = InvokeBuildPageQuery(adapter, "app.Users", 0, 10);

        Assert.Equal("SELECT * FROM APP.USERS ORDER BY ID OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleQuotesNonSimpleIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "Created At" });

        var query = InvokeBuildPageQuery(adapter, "app.User Audit", 5, 10);

        Assert.Equal("SELECT * FROM APP.\"User Audit\" ORDER BY \"Created At\" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleQuotesIdentifiersStartingWithUnderscore()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "_SortKey" });

        var query = InvokeBuildPageQuery(adapter, "app._Audit", 0, 10);

        Assert.Equal("SELECT * FROM APP.\"_Audit\" ORDER BY \"_SortKey\" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleAllowUnorderedDoesNotOrderByFirstColumn()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            allowUnordered: true);

        var query = InvokeBuildPageQuery(adapter, "app.Users", 0, 10);

        Assert.Equal("SELECT * FROM APP.USERS OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlFoldsSimpleIdentifiersToLowercase()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "CreatedUtc" });

        var query = InvokeBuildPageQuery(adapter, "Public.Users", 0, 10);

        Assert.Equal("SELECT * FROM public.users ORDER BY createdutc LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlPreservesExplicitQuotedIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "\"CreatedUtc\"" });

        var query = InvokeBuildPageQuery(adapter, "\"Public\".\"Users\"", 0, 10);

        Assert.Equal("SELECT * FROM \"Public\".\"Users\" ORDER BY \"CreatedUtc\" LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlPreservesDotsInsideExplicitQuotedIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "\"Created.Utc\"" });

        var query = InvokeBuildPageQuery(adapter, "\"Tenant.v1\".\"Users.Current\"", 0, 10);

        Assert.Equal("SELECT * FROM \"Tenant.v1\".\"Users.Current\" ORDER BY \"Created.Utc\" LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_PostgreSqlQuotesReservedIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "user" });

        var query = InvokeBuildPageQuery(adapter, "public.order", 0, 10);

        Assert.Equal("SELECT * FROM public.\"order\" ORDER BY \"user\" LIMIT 10 OFFSET 0", query);
    }

    [Fact]
    public void BuildPageQuery_SqlServerPreservesDotsInsideBracketedIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "[Created.Utc]" });

        var query = InvokeBuildPageQuery(adapter, "[tenant.v1].[Users.Current]", 0, 10);

        Assert.Equal("SELECT * FROM [tenant.v1].[Users.Current] ORDER BY [Created.Utc] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OraclePreservesExplicitQuotedIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "\"CreatedUtc\"" });

        var query = InvokeBuildPageQuery(adapter, "\"App\".\"Users\"", 0, 10);

        Assert.Equal("SELECT * FROM \"App\".\"Users\" ORDER BY \"CreatedUtc\" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_OracleQuotesReservedIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "ORDER" });

        var query = InvokeBuildPageQuery(adapter, "app.USER", 0, 10);

        Assert.Equal("SELECT * FROM APP.\"USER\" ORDER BY \"ORDER\" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BulkDestinationName_PostgreSqlFoldsSimpleIdentifiersBeforeProviderBulkCopyQuotesThem()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");

        var normalized = InvokeNormalizePostgreSqlBulkDestinationTableName(adapter, "Public.Users");

        Assert.Equal("public.users", normalized);
    }

    [Fact]
    public void BulkDestinationName_PostgreSqlPreservesDotsInsideExplicitQuotedSegments()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");

        var normalized = InvokeNormalizePostgreSqlBulkDestinationTableName(adapter, "\"tenant.v1\".\"Rows.Current\"");

        Assert.Equal("\"tenant.v1\".\"Rows.Current\"", normalized);
    }

    [Fact]
    public void BulkDestinationName_SqlServerQuotesDestinationPath()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True");

        var normalized = InvokeNormalizeSqlServerBulkDestinationTableName(adapter, "[tenant.v1].[Rows.Current]");

        Assert.Equal("[tenant.v1].[Rows.Current]", normalized);
    }

    [Fact]
    public void BulkDestinationName_MySqlTranslatesPlannerQuotesToBackticks()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.MySql,
            "Server=localhost;Database=db;User ID=u;Password=p;SslMode=Required;AllowLoadLocalInfile=True");

        var normalized = InvokeNormalizeMySqlBulkDestinationTableName(adapter, "\"tenant.v1\".\"Rows.Current\"");

        Assert.Equal("`tenant.v1`.`Rows.Current`", normalized);
    }

    [Fact]
    public void RegularOperationConnectionString_MySqlRemovesBulkOnlyLocalInfileOptions()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.MySql,
            "Server=localhost;Database=db;User ID=u;Password=p;SslMode=Required;AllowLoadLocalInfile=True;Allow Load Local Infile=True");

        var normalized = InvokeResolveMySqlRegularOperationConnectionString(adapter);

        Assert.Contains("Server=localhost", normalized, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Database=db", normalized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("AllowLoadLocalInfile", normalized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Allow Load Local Infile", normalized, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidatePage_MySqlRequiresLocalInfileBeforeClearDestination()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.MySql,
            "Server=localhost;Database=db;User ID=u;Password=p;SslMode=Required");
        using var page = new DataTable("Users");
        page.Columns.Add("Id", typeof(int));
        page.Rows.Add(1);

        var exception = Assert.Throws<InvalidOperationException>(() => adapter.ValidatePage(new DbaTableCopyDefinition("Users", "Users"), page));

        Assert.Contains("AllowLoadLocalInfile=true", exception.Message);
    }

    [Fact]
    public void ValidatePage_MySqlAllowsLocalInfileBeforeClearDestination()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.MySql,
            "Server=localhost;Database=db;User ID=u;Password=p;SslMode=Required;AllowLoadLocalInfile=True");
        using var page = new DataTable("Users");
        page.Columns.Add("Id", typeof(int));
        page.Rows.Add(1);

        adapter.ValidatePage(new DbaTableCopyDefinition("Users", "Users"), page);
    }

    [Fact]
    public void BulkDestinationName_OracleQuotesDestinationPath()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p");

        var normalized = InvokeNormalizeOracleBulkDestinationTableName(adapter, "app.Order Details");

        Assert.Equal("APP.\"Order Details\"", normalized);
    }

    [Theory]
    [InlineData("[Rows.Current]", "\"Rows.Current\"")]
    [InlineData("`Rows.Current`", "\"Rows.Current\"")]
    public void BulkDestinationName_SQLiteStripsAlternativeIdentifierDelimitersBeforeQuoting(string destinationTableName, string expected)
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.SQLite,
            "Data Source=:memory:");

        var normalized = InvokeNormalizeSQLiteBulkDestinationTableName(adapter, destinationTableName);

        Assert.Equal(expected, normalized);
    }

    [Fact]
    public void BulkPage_PostgreSqlNormalizesSimpleColumnNamesBeforeProviderBulkCopyQuotesThem()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("\"CreatedUtc\"", typeof(DateTime));
        page.Columns.Add("Created At", typeof(string));

        var normalized = InvokeNormalizePostgreSqlBulkPage(adapter, page, "Users");

        Assert.Equal(new[] { "displayname", "CreatedUtc", "Created At" }, normalized.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
        Assert.Equal("DisplayName", page.Columns[0].ColumnName);
    }

    [Fact]
    public void BulkPage_PostgreSqlRehydratesProviderNeutralNetworkValues()
    {
        using var page = new DataTable("Networks");
        page.Columns.Add("Subnet", typeof(DbaIpNetwork));
        var expected = new DbaIpNetwork(IPAddress.Parse("198.51.100.0"), 24);
        page.Rows.Add(expected);

        using DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, "Networks");

        Assert.Equal(typeof(System.Net.IPNetwork), normalized.Columns[0].DataType);
        var providerValue = Assert.IsType<System.Net.IPNetwork>(normalized.Rows[0][0]);
        Assert.Equal(expected.Address, providerValue.BaseAddress);
        Assert.Equal(expected.PrefixLength, providerValue.PrefixLength);
        Assert.Equal(expected, Assert.IsType<DbaIpNetwork>(PostgreSqlTableCopyAdapter.NormalizeProviderValue(providerValue)));
    }

    [Fact]
    public void BulkPage_PostgreSqlRehydratesProviderNeutralYearMonthIntervals()
    {
        using var page = new DataTable("Periods");
        page.Columns.Add("Period", typeof(DbaYearMonthInterval));
        var expected = new DbaYearMonthInterval(-27);
        page.Rows.Add(expected);

        using DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, "Periods");

        Assert.Equal(typeof(NpgsqlTypes.NpgsqlInterval), normalized.Columns[0].DataType);
        var providerValue = Assert.IsType<NpgsqlTypes.NpgsqlInterval>(normalized.Rows[0][0]);
        Assert.Equal(expected.TotalMonths, providerValue.Months);
        Assert.Equal(0, providerValue.Days);
        Assert.Equal(0, providerValue.Time);
        Assert.Equal(expected, page.Rows[0][0]);
    }

    [Fact]
    public void BulkPage_PostgreSqlRoundTripsCalendarIntervalComponents()
    {
        using var page = new DataTable("Periods");
        page.Columns.Add("Period", typeof(DbaCalendarInterval));
        var expected = new DbaCalendarInterval(-13, 5, 12_345_678);
        page.Rows.Add(expected);

        using DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, "Periods");

        Assert.Equal(typeof(NpgsqlInterval), normalized.Columns[0].DataType);
        NpgsqlInterval providerValue = Assert.IsType<NpgsqlInterval>(normalized.Rows[0][0]);
        Assert.Equal(expected.Months, providerValue.Months);
        Assert.Equal(expected.Days, providerValue.Days);
        Assert.Equal(expected.Microseconds, providerValue.Time);
        Assert.Equal(expected, PostgreSqlTableCopyAdapter.NormalizeInterval(providerValue));
        NpgsqlInterval parameter = Assert.IsType<NpgsqlInterval>(PostgreSqlTableCopyAdapter.GetPageParameterValue(expected));
        Assert.Equal(providerValue, parameter);
    }

    [Fact]
    public void BulkPage_PostgreSqlRejectsYearMonthIntervalsOutsideProviderRange()
    {
        using var page = new DataTable("Periods");
        page.Columns.Add("Period", typeof(DbaYearMonthInterval));
        page.Rows.Add(new DbaYearMonthInterval((long)int.MaxValue + 1));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, "Periods"));

        Assert.Contains("exceeds", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("native interval range", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BulkPage_PostgreSqlPreservesUtcDateTimeModeWhenRehydratingNetworkValues()
    {
        using var page = new DataTable("Networks");
        page.Columns.Add("Subnet", typeof(DbaIpNetwork));
        DataColumn occurredAt = page.Columns.Add("OccurredAt", typeof(DateTime));
        occurredAt.DateTimeMode = DataSetDateTime.Utc;
        DateTime expected = new(2026, 9, 20, 12, 30, 0, DateTimeKind.Utc);
        page.Rows.Add(new DbaIpNetwork(IPAddress.Parse("198.51.100.0"), 24), expected);

        using DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, "Networks");

        Assert.Equal(DataSetDateTime.Utc, normalized.Columns[1].DateTimeMode);
        Assert.Equal(DateTimeKind.Utc, Assert.IsType<DateTime>(normalized.Rows[0][1]).Kind);
        Assert.Equal(expected, normalized.Rows[0][1]);
    }

    [Fact]
    public void BulkPage_PostgreSqlNormalizesDateTimeOffsetValuesToUtc()
    {
        using var page = new DataTable("Events");
        page.Columns.Add("OccurredAt", typeof(DateTimeOffset));
        page.Columns.Add("DynamicInstant", typeof(object));
        var sourceInstant = new DateTimeOffset(2026, 9, 20, 12, 30, 0, TimeSpan.FromHours(2));
        page.Rows.Add(sourceInstant, sourceInstant);

        using DataTable normalized = DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, "Events");

        var instant = Assert.IsType<DateTimeOffset>(normalized.Rows[0][0]);
        Assert.Equal(TimeSpan.Zero, instant.Offset);
        Assert.Equal(new DateTimeOffset(2026, 9, 20, 10, 30, 0, TimeSpan.Zero), instant);
        Assert.Equal(TimeSpan.Zero, Assert.IsType<DateTimeOffset>(normalized.Rows[0][1]).Offset);
        Assert.Equal(TimeSpan.FromHours(2), Assert.IsType<DateTimeOffset>(page.Rows[0][0]).Offset);
        Assert.Equal(TimeSpan.FromHours(2), Assert.IsType<DateTimeOffset>(page.Rows[0][1]).Offset);
    }

    [Fact]
    public void BulkPage_PostgreSqlFoldsSimpleColumnNamesForQuotedDestinationTable()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("\"CreatedUtc\"", typeof(DateTime));

        var normalized = InvokeNormalizePostgreSqlBulkPage(adapter, page, "\"Public\".\"Users\"");

        Assert.Equal(new[] { "displayname", "CreatedUtc" }, normalized.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public void BulkPage_PostgreSqlFoldsSimpleColumnNamesWhenOnlySchemaIsQuoted()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("\"CreatedUtc\"", typeof(DateTime));

        var normalized = InvokeNormalizePostgreSqlBulkPage(adapter, page, "\"TenantA\".Users");

        Assert.Equal(new[] { "displayname", "CreatedUtc" }, normalized.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
    }

    [Fact]
    public void ValidatePage_PostgreSqlRejectsDuplicateNormalizedColumnsBeforeWrite()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p");
        using var page = new DataTable("Users");
        page.Columns.Add("DisplayName", typeof(string));
        page.Columns.Add("displayname", typeof(string));

        var exception = Assert.Throws<InvalidOperationException>(() => adapter.ValidatePage(new DbaTableCopyDefinition("Users", "Users"), page));

        Assert.Contains("duplicate destination column 'displayname'", exception.Message);
    }

    [Fact]
    public void MissingTableDetection_UsesProviderErrorCodes()
    {
        Assert.True(SqlServerTableCopyAdapter.IsMissingTableErrorNumber(208));
        Assert.False(SqlServerTableCopyAdapter.IsMissingTableErrorNumber(207));
        Assert.True(PostgreSqlTableCopyAdapter.IsMissingTableSqlState("42P01"));
        Assert.True(PostgreSqlTableCopyAdapter.IsMissingTableSqlState("3F000"));
        Assert.False(PostgreSqlTableCopyAdapter.IsMissingTableSqlState("42703"));
        Assert.True(MySqlTableCopyAdapter.IsMissingTableErrorCode(MySqlConnector.MySqlErrorCode.NoSuchTable));
        Assert.False(MySqlTableCopyAdapter.IsMissingTableErrorCode(MySqlConnector.MySqlErrorCode.BadFieldError));
        Assert.True(OracleTableCopyAdapter.IsMissingTableErrorNumber(942));
        Assert.False(OracleTableCopyAdapter.IsMissingTableErrorNumber(904));
        Assert.True(SQLiteTableCopyAdapter.IsMissingTableError(1, "SQLite Error 1: 'no such table: MissingRows'."));
        Assert.False(SQLiteTableCopyAdapter.IsMissingTableError(1, "SQLite Error 1: 'no such column: BadKey'."));
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.SqlServer)]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [InlineData(DbaTableCopyProvider.Oracle)]
    [InlineData(DbaTableCopyProvider.SQLite)]
    public void MissingTableDetection_UsesSanitizedProviderClassification(DbaTableCopyProvider provider)
    {
        var adapter = CreateAdapter(provider, GetTestConnectionString(provider));
        var exception = new DbaQueryExecutionException(
            "Failed to execute query.",
            "SELECT * FROM MissingRows",
            new InvalidOperationException("provider-secret"),
            providerErrorCode: null,
            providerSqlState: null,
            providerErrorKind: DbaProviderErrorKind.MissingTable);

        Assert.True(((IDbaTableCopyMissingTableClassifier)adapter).IsMissingTableException(exception));
        Assert.DoesNotContain("provider-secret", exception.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.SqlServer)]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [InlineData(DbaTableCopyProvider.Oracle)]
    [InlineData(DbaTableCopyProvider.SQLite)]
    public void ProviderOptions_PropagateCommandTimeout(DbaTableCopyProvider provider)
    {
        var adapter = CreateAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = provider,
            ConnectionString = GetTestConnectionString(provider),
            CommandTimeout = 41
        });

        Assert.Equal(41, adapter.CommandTimeout);
    }

    [Theory]
    [InlineData(DbaTableCopyProvider.SqlServer)]
    [InlineData(DbaTableCopyProvider.PostgreSql)]
    [InlineData(DbaTableCopyProvider.MySql)]
    [InlineData(DbaTableCopyProvider.Oracle)]
    [InlineData(DbaTableCopyProvider.SQLite)]
    public void ProviderOptions_RejectNegativeCommandTimeout(DbaTableCopyProvider provider)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = provider,
            ConnectionString = GetTestConnectionString(provider),
            CommandTimeout = -1
        }));
    }

    [Fact]
    public void BuildPageQuery_SqlServerStripsDelimitersBeforeQuotingIdentifiers()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "[Id]" });

        var query = InvokeBuildPageQuery(adapter, "[dbo].[Rows]", 0, 10);

        Assert.Equal("SELECT * FROM [dbo].[Rows] ORDER BY [Id] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    [Fact]
    public void BuildPageQuery_DeduplicationUsesNamespacedRankAlias()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.PostgreSql,
            "Host=localhost;Database=db;Username=u;Password=p",
            new[] { "ProbeName" });

        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "BuildPageQuery");
        var query = (string)method.Invoke(
            adapter,
            new object?[]
            {
                "Public.ProbeIndex",
                new[] { "ProbeName" },
                new DbaTableCopySourceOptions(new[] { "ProbeName" }, new[] { "LastCompletedUtc" }),
                0L,
                10
            })!;

        Assert.Contains("__DbaXR_", query);
        Assert.DoesNotContain("__DbaXCRank_62D977CD", query);
    }

    [Fact]
    public async Task ReadPageAsync_DeduplicationPreservesSourceColumnNamedLikeRankAlias()
    {
        var sourcePath = CreateTempDatabasePath();
        try
        {
            using (var sqlite = new SQLite())
            {
                sqlite.ExecuteNonQuery(
                    sourcePath,
                    "CREATE TABLE SourceRows (Id INTEGER NOT NULL, ProbeName TEXT NOT NULL, LastCompletedUtcMs INTEGER NOT NULL, \"__DbaXCRank_62D977CD\" TEXT NOT NULL);");
                sqlite.ExecuteNonQuery(
                    sourcePath,
                    "INSERT INTO SourceRows (Id, ProbeName, LastCompletedUtcMs, \"__DbaXCRank_62D977CD\") VALUES (1, 'Server1', 10, 'source-a'), (2, 'Server1', 20, 'source-b');");
            }

            var adapter = CreateAdapter(
                DbaTableCopyProvider.SQLite,
                "Data Source=" + sourcePath,
                new[] { "ProbeName" });
            var definition = new DbaTableCopyDefinition(
                "SourceRows",
                "DestinationRows",
                new[] { "ProbeName" },
                SourceOptions: new DbaTableCopySourceOptions(new[] { "ProbeName" }, new[] { "LastCompletedUtcMs" }));

            using var page = await adapter.ReadPageAsync(new DbaTableCopyPageRequest(definition, continuationToken: null, pageSize: 10));

            Assert.Equal(1, page.Data.Rows.Count);
            Assert.Contains("__DbaXCRank_62D977CD", page.Data.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
            Assert.Equal("source-b", page.Data.Rows[0]["__DbaXCRank_62D977CD"]);
            Assert.DoesNotContain(page.Data.Columns.Cast<DataColumn>(), static column => column.ColumnName.StartsWith("__DbaXR_", StringComparison.Ordinal));
        }
        finally
        {
            DeleteIfExists(sourcePath);
        }
    }

    [Fact]
    public void BuildCountQuery_DeduplicationAliasesConstantForSqlServerDerivedTable()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "ProbeName" });

        var query = InvokeBuildCountQuery(
            adapter,
            "[dbo].[ProbeIndex]",
            new DbaTableCopySourceOptions(new[] { "ProbeName" }, new[] { "LastCompletedUtcMs" }, true));

        Assert.Equal("SELECT COUNT_BIG(*) FROM (SELECT 1 AS dbax_key FROM [dbo].[ProbeIndex] GROUP BY LOWER([ProbeName])) dbax_source_keys", query);
    }

    [Fact]
    public void BuildPageQuery_OracleDeduplicationRankAliasFitsLegacyIdentifierLimit()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.Oracle,
            "Data Source=oracle;User Id=u;Password=p",
            new[] { "ProbeName" });

        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "BuildPageQuery");
        var query = (string)method.Invoke(
            adapter,
            new object?[]
            {
                "App.ProbeIndex",
                new[] { "ProbeName" },
                new DbaTableCopySourceOptions(new[] { "ProbeName" }, new[] { "LastCompletedUtc" }),
                0L,
                10
            })!;

        var rankAliasStart = query.IndexOf("\"__DbaXR_", StringComparison.Ordinal);
        Assert.True(rankAliasStart >= 0);
        var rankAliasEnd = query.IndexOf('"', rankAliasStart + 1);
        Assert.True(rankAliasEnd > rankAliasStart);
        Assert.True(rankAliasEnd - rankAliasStart - 1 <= 30);
        Assert.DoesNotContain("__DbaXCopyRank_62D977CD8E7A4BC08D1A73B5197F33D4", query);
    }

    [Fact]
    public async Task CopyAsync_SqlServerSameTableProtectionBlocksSchemaQualifiedAliases()
    {
        var request = new DbaProviderTableCopyRequest
        {
            Source = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Server=.;Database=tempdb;Integrated Security=True"
            },
            Destination = new DbaProviderTableCopyAdapterOptions
            {
                Provider = DbaTableCopyProvider.SqlServer,
                ConnectionString = "Data Source=localhost;Initial Catalog=tempdb;Integrated Security=True"
            },
            Definitions = new[]
            {
                new DbaTableCopyDefinition("dbo.Users", "[dbo].[Users]", new[] { "Id" })
            }
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => CreateRunner().CopyAsync(request));
        Assert.Contains("Refusing to copy provider table", ex.Message);
    }

    [Fact]
    public void BuildPageQuery_UsesDefinitionOrderColumnsWhenProvided()
    {
        var adapter = CreateAdapter(
            DbaTableCopyProvider.SqlServer,
            "Server=.;Database=tempdb;Integrated Security=True",
            new[] { "FallbackId" });

        var query = InvokeBuildPageQuery(adapter, "dbo.Users", 0, 10, new[] { "DefinitionId" });

        Assert.Equal("SELECT * FROM [dbo].[Users] ORDER BY [DefinitionId] OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", query);
    }

    private static string InvokeBuildPageQuery(DbaProviderTableCopyAdapterBase adapter, string tableName, long offset, int pageSize, IReadOnlyList<string>? orderByColumns = null)
    {
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("BuildPageQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "BuildPageQuery");

        return (string)method.Invoke(adapter, new object?[] { tableName, orderByColumns, null, offset, pageSize })!;
    }

    private static string InvokeBuildCountQuery(DbaProviderTableCopyAdapterBase adapter, string tableName, DbaTableCopySourceOptions? sourceOptions)
    {
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("BuildCountQuery", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "BuildCountQuery");

        return (string)method.Invoke(adapter, new object?[] { tableName, sourceOptions })!;
    }

    private static string InvokeNormalizePostgreSqlBulkDestinationTableName(DbaProviderTableCopyAdapterBase adapter, string destinationTableName)
        => DbaPostgreSqlBulkCopyNormalizer.NormalizeDestinationTableName(destinationTableName);

    private static string InvokeNormalizeSqlServerBulkDestinationTableName(DbaProviderTableCopyAdapterBase adapter, string destinationTableName)
        => InvokeNormalizeQuotedBulkDestinationTableName(adapter, destinationTableName);

    private static string InvokeNormalizeMySqlBulkDestinationTableName(DbaProviderTableCopyAdapterBase adapter, string destinationTableName)
        => InvokeNormalizeQuotedBulkDestinationTableName(adapter, destinationTableName);

    private static string InvokeResolveMySqlRegularOperationConnectionString(DbaProviderTableCopyAdapterBase adapter)
    {
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("ResolveMySqlRegularOperationConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "ResolveMySqlRegularOperationConnectionString");

        return (string)method.Invoke(adapter, Array.Empty<object?>())!;
    }

    private static string InvokeResolveSQLiteConnectionString(SQLiteTableCopyAdapter adapter)
    {
        var method = typeof(SQLiteTableCopyAdapter).GetMethod("ResolveSQLiteConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(SQLiteTableCopyAdapter), "ResolveSQLiteConnectionString");

        return (string)method.Invoke(adapter, Array.Empty<object?>())!;
    }

    private static string InvokeNormalizeOracleBulkDestinationTableName(DbaProviderTableCopyAdapterBase adapter, string destinationTableName)
        => InvokeNormalizeQuotedBulkDestinationTableName(adapter, destinationTableName);

    private static string InvokeNormalizeSQLiteBulkDestinationTableName(DbaProviderTableCopyAdapterBase adapter, string destinationTableName)
    {
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("NormalizeSQLiteBulkDestinationTableName", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "NormalizeSQLiteBulkDestinationTableName");

        return (string)method.Invoke(adapter, new object?[] { destinationTableName })!;
    }

    private static DataTable InvokeNormalizePostgreSqlBulkPage(DbaProviderTableCopyAdapterBase adapter, DataTable page, string destinationTableName)
        => DbaPostgreSqlBulkCopyNormalizer.NormalizePage(page, destinationTableName);

    private static string InvokeNormalizeQuotedBulkDestinationTableName(DbaProviderTableCopyAdapterBase adapter, string destinationTableName)
    {
        var method = typeof(DbaProviderTableCopyAdapterBase).GetMethod("NormalizeQuotedBulkDestinationTableName", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(nameof(DbaProviderTableCopyAdapterBase), "NormalizeQuotedBulkDestinationTableName");

        return (string)method.Invoke(adapter, new object?[] { destinationTableName })!;
    }

    private static DbaProviderTableCopyRunner CreateRunner()
        => new(CreateAdapter, CreateAdapter);

    private static DbaProviderTableCopyAdapterBase CreateAdapter(DbaProviderTableCopyAdapterOptions options)
        => options.Provider switch
        {
            DbaTableCopyProvider.SqlServer => new SqlServerTableCopyAdapter(options),
            DbaTableCopyProvider.PostgreSql => new PostgreSqlTableCopyAdapter(options),
            DbaTableCopyProvider.MySql => new MySqlTableCopyAdapter(options),
            DbaTableCopyProvider.Oracle => new OracleTableCopyAdapter(options),
            DbaTableCopyProvider.SQLite => new SQLiteTableCopyAdapter(options),
            _ => throw new NotSupportedException($"Provider '{options.Provider}' is not supported.")
        };

    private static DbaProviderTableCopyAdapterBase CreateAdapter(
        DbaTableCopyProvider provider,
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        bool treatMissingTablesAsEmpty = false)
        => provider switch
        {
            DbaTableCopyProvider.SqlServer => new SqlServerTableCopyAdapter(connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty: treatMissingTablesAsEmpty),
            DbaTableCopyProvider.PostgreSql => new PostgreSqlTableCopyAdapter(connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty),
            DbaTableCopyProvider.MySql => new MySqlTableCopyAdapter(connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty),
            DbaTableCopyProvider.Oracle => new OracleTableCopyAdapter(connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty),
            DbaTableCopyProvider.SQLite => new SQLiteTableCopyAdapter(connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty),
            _ => throw new NotSupportedException($"Provider '{provider}' is not supported.")
        };

    private static string GetTestConnectionString(DbaTableCopyProvider provider)
        => provider switch
        {
            DbaTableCopyProvider.SqlServer => "Server=.;Database=tempdb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True",
            DbaTableCopyProvider.PostgreSql => "Host=localhost;Database=db;Username=u;Password=p;SslMode=Require",
            DbaTableCopyProvider.MySql => "Server=localhost;Database=db;User ID=u;Password=p;SslMode=Required;AllowLoadLocalInfile=True",
            DbaTableCopyProvider.Oracle => "Data Source=localhost/service;User Id=u;Password=p",
            DbaTableCopyProvider.SQLite => "Data Source=:memory:",
            _ => throw new ArgumentOutOfRangeException(nameof(provider))
        };

    private static void CreateHistoryTables(SQLite sqlite, string path)
    {
        sqlite.ExecuteNonQuery(path, "CREATE TABLE ProbeResults (ResultId INTEGER NOT NULL PRIMARY KEY, ProbeName TEXT NOT NULL, IsMaintenance INTEGER NOT NULL);");
        sqlite.ExecuteNonQuery(path, "CREATE TABLE ProbeResultMetadata (ResultId INTEGER NOT NULL, MetaKey TEXT NOT NULL, MetaValue TEXT NOT NULL, PRIMARY KEY (ResultId, MetaKey));");
    }

    private static string ComputeContentHash(DataTable table, params string[] columns)
        => ComputeContentHash(table, valueNormalizer: null, columns);

    private static string ComputeContentHash(
        DataTable table,
        IDbaTableCopyContentValueNormalizer? valueNormalizer,
        params string[] columns)
    {
        var hasherType = typeof(DbaTableCopyDefinition).Assembly.GetType(
            "DBAClientX.DataMovement.DbaTableCopyContentHasher",
            throwOnError: true)!;
        using var hasher = (IDisposable)(Activator.CreateInstance(
            hasherType,
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            args: new object?[] { null },
            culture: null) ?? throw new InvalidOperationException("Could not create the content hasher."));
        var add = hasherType.GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(hasherType.FullName, "Add");
        var hash = hasherType.GetProperty("Hash", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMemberException(hasherType.FullName, "Hash");
        add.Invoke(hasher, new object?[] { table, columns, CancellationToken.None, valueNormalizer });
        return Assert.IsType<string>(hash.GetValue(hasher));
    }

    private static void DeleteIfExists(string path)
    {
        foreach (var suffix in new[] { string.Empty, "-wal", "-shm", "-journal" })
        {
            var candidate = path + suffix;
            if (File.Exists(candidate))
            {
                File.Delete(candidate);
            }
        }
    }

    private static string CreateTempDatabasePath()
        => Path.Join(Path.GetTempPath(), Path.ChangeExtension(Path.GetRandomFileName(), ".db"));

    private static string CreateTempDatabasePath(string namePrefix)
        => Path.Join(Path.GetTempPath(), namePrefix + "-" + Path.ChangeExtension(Path.GetRandomFileName(), ".db"));
}
