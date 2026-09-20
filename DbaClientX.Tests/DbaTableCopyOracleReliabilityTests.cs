using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;
using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DbaClientX.Tests;

public sealed class DbaTableCopyOracleReliabilityTests
{
    [Theory]
    [InlineData(typeof(byte[]), OracleDbType.Raw)]
    [InlineData(typeof(string), OracleDbType.Varchar2)]
    [InlineData(typeof(long), OracleDbType.Int64)]
    [InlineData(typeof(ulong), OracleDbType.Decimal)]
    [InlineData(typeof(decimal), OracleDbType.Decimal)]
    [InlineData(typeof(DateTime), OracleDbType.TimeStamp)]
    [InlineData(typeof(DateTimeOffset), OracleDbType.TimeStampTZ)]
    [InlineData(typeof(TimeSpan), OracleDbType.IntervalDS)]
    [InlineData(typeof(Guid), OracleDbType.Raw)]
    public void CheckpointPageParameters_AreTypedFromDataColumns(Type dataType, OracleDbType expected)
    {
        Assert.Equal(expected, OracleTableCopyAdapter.GetPageParameterType(dataType));
    }

    [Theory]
    [InlineData(typeof(string), "CLOB", OracleDbType.Clob)]
    [InlineData(typeof(string), "NCLOB", OracleDbType.NClob)]
    [InlineData(typeof(byte[]), "BLOB", OracleDbType.Blob)]
    [InlineData(typeof(byte[]), "RAW", OracleDbType.Raw)]
    [InlineData(typeof(DateOnly), "DATE", OracleDbType.Date)]
    [InlineData(typeof(DateOnly), "TIMESTAMP(6)", OracleDbType.TimeStamp)]
    [InlineData(typeof(TimeOnly), "INTERVAL DAY(2) TO SECOND(6)", OracleDbType.IntervalDS)]
    [InlineData(typeof(bool), "NUMBER", OracleDbType.Decimal)]
    public void CheckpointPageParameters_UseDestinationLobMetadata(
        Type sourceType,
        string destinationType,
        OracleDbType expected)
    {
        Assert.Equal(expected, OracleTableCopyAdapter.GetPageParameterType(sourceType, destinationType));
    }

    [Fact]
    public void CheckpointContinuationToken_IsBoundAsClob()
    {
        var parameter = new OracleParameter();

        OracleTableCopyAdapter.ConfigureCheckpointParameter(parameter, "token");

        Assert.Equal(OracleDbType.Clob, parameter.OracleDbType);
    }

    [Fact]
    public void CheckpointCopyId_IsBoundWithThePublicCharacterLimit()
    {
        var parameter = new OracleParameter();

        OracleTableCopyAdapter.ConfigureCheckpointParameter(parameter, "copyId");

        Assert.Equal(OracleDbType.Varchar2, parameter.OracleDbType);
        Assert.Equal(128, parameter.Size);
    }

    [Fact]
    public void CheckpointPageValues_NormalizeUnsignedIntegerWithoutPrecisionLoss()
    {
        Assert.Equal(Convert.ToDecimal(ulong.MaxValue), OracleTableCopyAdapter.GetPageParameterValue(ulong.MaxValue));
    }

    [Fact]
    public void CheckpointPageValues_ConvertGuidToRawBytes()
    {
        var value = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

        Assert.Equal(value.ToByteArray(), OracleTableCopyAdapter.GetPageParameterValue(value));
    }

    [Fact]
    public void CheckpointPageValues_ConvertPostgreSqlTemporalValues()
    {
        var date = new DateOnly(2026, 9, 20);
        var time = new TimeOnly(12, 34, 56).Add(TimeSpan.FromTicks(7890));

        Assert.Equal(date.ToDateTime(TimeOnly.MinValue), OracleTableCopyAdapter.GetPageParameterValue(date));
        Assert.Equal(time.ToTimeSpan(), OracleTableCopyAdapter.GetPageParameterValue(time));
    }

    [Fact]
    public void ProviderValues_NormalizeYearMonthIntervalsWithoutLosingSemantics()
    {
        object normalized = OracleTableCopyAdapter.NormalizeProviderValue(new OracleIntervalYM(-27L));

        Assert.Equal(new DbaYearMonthInterval(-27), Assert.IsType<DbaYearMonthInterval>(normalized));
        var rebound = Assert.IsType<OracleIntervalYM>(OracleTableCopyAdapter.GetPageParameterValue(normalized));
        Assert.Equal(-27L, rebound.Value);
        Assert.Equal(OracleDbType.IntervalYM, OracleTableCopyAdapter.GetPageParameterType(normalized.GetType()));
    }

    [Fact]
    public async Task BoundedPages_NormalizeYearMonthIntervalsBeforeMaterialization()
    {
        using var source = new DataTable();
        source.Columns.Add("Period", typeof(OracleIntervalYM));
        source.Rows.Add(new OracleIntervalYM(27L));
        using DataTableReader reader = source.CreateDataReader();

        using DataTable page = await DbaTableCopyPageReader.ReadAsync(
            reader,
            maxBytes: 4096,
            fieldPayloadBytes: null,
            readFieldValue: ordinal => OracleTableCopyAdapter.NormalizeProviderValue(reader.GetValue(ordinal)),
            normalizedFieldType: ordinal => OracleTableCopyAdapter.GetNormalizedFieldType(reader.GetFieldType(ordinal)));

        Assert.Equal(new DbaYearMonthInterval(27), page.Rows[0]["Period"]);
        Assert.Equal(typeof(DbaYearMonthInterval), page.Columns["Period"]!.DataType);
    }

    [Theory]
    [InlineData(typeof(OracleIntervalYM), typeof(DbaYearMonthInterval))]
    [InlineData(typeof(OracleIntervalDS), typeof(TimeSpan))]
    [InlineData(typeof(OracleBinary), typeof(byte[]))]
    [InlineData(typeof(OracleBlob), typeof(byte[]))]
    [InlineData(typeof(OracleClob), typeof(string))]
    [InlineData(typeof(OracleXmlType), typeof(string))]
    [InlineData(typeof(OracleTimeStampTZ), typeof(DateTimeOffset))]
    public void ProviderSchemas_UseNormalizedManagedTypes(Type providerType, Type expected)
    {
        Assert.Equal(expected, OracleTableCopyAdapter.GetNormalizedFieldType(providerType));
    }

    [Theory]
    [InlineData(typeof(OracleBlob), "BLOB")]
    [InlineData(typeof(OracleClob), "CLOB")]
    [InlineData(typeof(OracleXmlType), "XMLTYPE")]
    public void BoundedPages_RejectProviderNativeLargeValuesBeforeMaterialization(Type providerType, string dataTypeName)
    {
        var exception = Assert.Throws<NotSupportedException>(() =>
            OracleTableCopyAdapter.ValidateBoundedFieldType(providerType, dataTypeName));

        Assert.Contains("Project it to text or binary", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ProviderValues_NormalizeSupportedOracleScalarTypes()
    {
        var timestamp = new DateTime(2026, 9, 20, 12, 34, 56, DateTimeKind.Unspecified);

        Assert.Equal(TimeSpan.FromHours(27), OracleTableCopyAdapter.NormalizeProviderValue(new OracleIntervalDS(TimeSpan.FromHours(27))));
        Assert.Equal(new byte[] { 1, 2, 3 }, OracleTableCopyAdapter.NormalizeProviderValue(new OracleBinary(new byte[] { 1, 2, 3 })));
        Assert.Equal(true, OracleTableCopyAdapter.NormalizeProviderValue(new OracleBoolean(true)));
        Assert.Equal(12.5m, OracleTableCopyAdapter.NormalizeProviderValue(new OracleDecimal(12.5m)));
        Assert.Equal(timestamp, OracleTableCopyAdapter.NormalizeProviderValue(new OracleDate(timestamp)));
        Assert.Equal("value", OracleTableCopyAdapter.NormalizeProviderValue(new OracleString("value")));
        Assert.Equal(timestamp, OracleTableCopyAdapter.NormalizeProviderValue(new OracleTimeStamp(timestamp)));
        Assert.Equal(
            new DateTimeOffset(timestamp, TimeSpan.FromHours(2)),
            OracleTableCopyAdapter.NormalizeProviderValue(new OracleTimeStampTZ(timestamp, "+02:00")));
    }

    [Fact]
    public void BulkPages_RehydrateYearMonthIntervalsForOracle()
    {
        using var page = new DataTable();
        page.Columns.Add("Id", typeof(long));
        page.Columns.Add("Period", typeof(object));
        page.Rows.Add(1L, new DbaYearMonthInterval(27));

        using DataTable normalized = Assert.IsType<DataTable>(OracleTableCopyAdapter.NormalizeBulkPage(page));

        Assert.Equal(typeof(OracleIntervalYM), normalized.Columns["Period"]!.DataType);
        Assert.Equal(27L, Assert.IsType<OracleIntervalYM>(normalized.Rows[0]["Period"]).Value);
        Assert.Equal(new DbaYearMonthInterval(27), page.Rows[0]["Period"]);
    }

    [Fact]
    public void BulkPages_RejectMixedYearMonthIntervalColumns()
    {
        using var page = new DataTable();
        page.Columns.Add("Period", typeof(object));
        page.Rows.Add(new DbaYearMonthInterval(27));
        page.Rows.Add("not an interval");

        var exception = Assert.Throws<InvalidOperationException>(() =>
            OracleTableCopyAdapter.NormalizeBulkPage(page));

        Assert.Contains("incompatible value", exception.Message);
    }

    [Theory]
    [InlineData(true, "1")]
    [InlineData(false, "0")]
    public void CheckpointPageValues_ConvertBooleansForNumericDestinations(bool value, string expected)
    {
        object converted = OracleTableCopyAdapter.GetPageParameterValue(value, OracleDbType.Decimal);

        Assert.Equal(decimal.Parse(expected), Assert.IsType<decimal>(converted));
    }

    [Fact]
    public void CheckpointDestinationMetadata_ResolvesDelimitedPhysicalColumnNames()
    {
        var destinationTypes = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["User"] = "VARCHAR2"
        };

        Assert.Equal(
            "VARCHAR2",
            OracleTableCopyAdapter.ResolveDestinationDataType(destinationTypes, "\"User\""));
        Assert.Throws<InvalidOperationException>(() =>
            OracleTableCopyAdapter.ResolveDestinationDataType(destinationTypes, "\"USER\""));
    }

    [Fact]
    public void SchemaPreflight_RejectsGeneratedAlwaysIdentity()
    {
        var columns = new[]
        {
            new DBAClientX.Metadata.DbaColumnInfo("APP", "ROWS", "ID", "NUMBER")
            {
                IsIdentity = true,
                IdentityGeneration = "ALWAYS"
            }
        };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            OracleTableCopyAdapter.ValidateProjectedIdentityColumns("APP.ROWS", new[] { "ID" }, columns));

        Assert.Contains("GENERATED ALWAYS", exception.Message);
    }

    [Theory]
    [InlineData("BY DEFAULT")]
    [InlineData("BY DEFAULT ON NULL")]
    public void SchemaPreflight_AllowsGeneratedByDefaultIdentity(string generation)
    {
        var columns = new[]
        {
            new DBAClientX.Metadata.DbaColumnInfo("APP", "ROWS", "ID", "NUMBER")
            {
                IsIdentity = true,
                IdentityGeneration = generation
            }
        };

        OracleTableCopyAdapter.ValidateProjectedIdentityColumns("APP.ROWS", new[] { "ID" }, columns);
    }

    [Fact]
    public void TableCopyMetadata_UsesExactPhysicalOwnerAndTableOnly()
    {
        using var command = new OracleCommand();

        DBAClientX.Oracle.AddExactTableCopyMetadataParameters(command.Parameters, "App", "Rows");

        Assert.Equal("App", command.Parameters["schemaNameExact"].Value);
        Assert.True(command.Parameters["schemaNameNormalized"].Value is null or DBNull);
        Assert.Equal("Rows", command.Parameters["tableNameExact"].Value);
        Assert.True(command.Parameters["tableNameNormalized"].Value is null or DBNull);
    }

    [Fact]
    public void TableCopyMetadata_EnrichesIdentityGenerationByExactColumnName()
    {
        var columns = new List<DbaColumnInfo>
        {
            new("App", "Rows", "Id", "NUMBER") { IsIdentity = true, IdentityGeneration = "IDENTITY" },
            new("App", "Rows", "ID", "NUMBER") { IsIdentity = true, IdentityGeneration = "IDENTITY" }
        };

        DBAClientX.Oracle.ApplyIdentityGenerations(
            columns,
            new Dictionary<string, string>(StringComparer.Ordinal) { ["Id"] = "ALWAYS" });

        Assert.Equal("ALWAYS", columns[0].IdentityGeneration);
        Assert.Equal("IDENTITY", columns[1].IdentityGeneration);
    }

    [Fact]
    public void CheckpointPageParameters_RejectIncompatibleTimeOnlyDestination()
    {
        var exception = Assert.Throws<NotSupportedException>(() =>
            OracleTableCopyAdapter.GetPageParameterType(typeof(TimeOnly), "DATE"));

        Assert.Contains("INTERVAL DAY TO SECOND", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckpointPageParameters_RejectDateOnlyTimeZoneDestination()
    {
        Assert.Throws<NotSupportedException>(() =>
            OracleTableCopyAdapter.GetPageParameterType(typeof(DateOnly), "TIMESTAMP(6) WITH TIME ZONE"));
    }
}
