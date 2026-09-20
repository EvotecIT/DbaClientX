using DBAClientX;
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
    [InlineData(typeof(Guid), OracleDbType.Blob)]
    public void CheckpointPageParameters_AreTypedFromDataColumns(Type dataType, OracleDbType expected)
    {
        Assert.Equal(expected, OracleTableCopyAdapter.GetPageParameterType(dataType));
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
}
