using DBAClientX;
using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;

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
