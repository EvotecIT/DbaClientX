using System.Data;
using System.Collections;
using System.Collections.ObjectModel;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using DBAClientX;
using DBAClientX.PowerShell;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public class InvokeDbaXQueryCmdletTests
{
    [Fact]
    public void DataTableReturnType_EmitsSingleDataTable()
    {
        using var table = CreateTable();
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DataTableSqlServer(table);

        try
        {
            var results = InvokeQuery(ReturnType.DataTable);

            var result = Assert.Single(results);
            var resultTable = Assert.IsType<DataTable>(result.BaseObject);
            Assert.Equal(2, resultTable.Rows.Count);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    [Fact]
    public void DataRowReturnType_EmitsSingleDataRow()
    {
        using var table = CreateTable();
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DataTableSqlServer(table);

        try
        {
            var results = InvokeQuery(ReturnType.DataRow);

            var result = Assert.Single(results);
            Assert.IsType<DataRow>(result.BaseObject);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    [Theory]
    [InlineData(typeof(CmdletIInvokeDbaXQuery))]
    [InlineData(typeof(CmdletInvokeDbaXMySql))]
    [InlineData(typeof(CmdletInvokeDbaXOracle))]
    [InlineData(typeof(CmdletInvokeDbaXPostgreSql))]
    [InlineData(typeof(CmdletInvokeDbaXSQLite))]
    [InlineData(typeof(CmdletInvokeDbaXQueryStream))]
    [InlineData(typeof(CmdletInvokeDbaXStoredProcedure))]
    public void QueryCmdlets_DefaultToAllRowPowerShellObjects(Type cmdletType)
    {
        var cmdlet = Activator.CreateInstance(cmdletType);
        var returnType = cmdletType.GetProperty("ReturnType")!.GetValue(cmdlet);

        Assert.Equal(ReturnType.PSObject, returnType);
    }

    [Theory]
    [InlineData(null, 37)]
    [InlineData(0, 0)]
    [InlineData(12, 12)]
    public void QueryTimeout_AppliesOnlyWhenExplicitlyBound(int? queryTimeout, int expectedTimeout)
    {
        using var table = CreateTable();
        var sqlServer = new TimeoutCaptureSqlServer(table);
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => sqlServer;

        try
        {
            InvokeQuery(ReturnType.DataTable, queryTimeout);

            Assert.Equal(expectedTimeout, sqlServer.AppliedTimeout);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    [Fact]
    public void AsDataReader_EmitsSingleLiveReaderWithoutEnumeratingIt()
    {
        using var table = CreateTable();
        var ownedReader = new DbaDataReader(table.CreateDataReader());
        var sqlServer = new DataReaderSqlServer(ownedReader);
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => sqlServer;

        try
        {
            var results = InvokeReader(queryTimeout: 11, parameters: new Hashtable { ["MinimumId"] = 1 });

            var result = Assert.Single(results);
            var reader = Assert.IsType<DbaDataReader>(result.BaseObject);
            Assert.Same(ownedReader, reader);
            Assert.False(reader.IsClosed);
            Assert.True(reader.Read());
            Assert.Equal(1, reader.GetInt32(0));
            Assert.Equal(1, sqlServer.QueryReaderCalls);
            Assert.Equal(11, sqlServer.CommandTimeout);
            Assert.Equal(1, sqlServer.ReceivedParameters!["MinimumId"]);
            Assert.True(sqlServer.ReceivedCancellationToken.CanBeCanceled);
            Assert.True(new SqlConnectionStringBuilder(sqlServer.ReceivedConnectionString).TrustServerCertificate);

            reader.Dispose();
            Assert.True(reader.IsClosed);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
            ownedReader.Dispose();
        }
    }

    [Fact]
    public void AsDataReader_IsBindableInBufferedQueriesWhileRetainingTheReaderSet()
    {
        var readerParameterSets = typeof(CmdletIInvokeDbaXQuery)
            .GetProperty(nameof(CmdletIInvokeDbaXQuery.AsDataReader))!
            .GetCustomAttributes(typeof(ParameterAttribute), inherit: false)
            .Cast<ParameterAttribute>()
            .Select(attribute => attribute.ParameterSetName)
            .ToArray();
        var streamParameterSets = typeof(CmdletIInvokeDbaXQuery)
            .GetProperty(nameof(CmdletIInvokeDbaXQuery.Stream))!
            .GetCustomAttributes(typeof(ParameterAttribute), inherit: false)
            .Cast<ParameterAttribute>()
            .Select(attribute => attribute.ParameterSetName)
            .ToArray();
        var returnTypeParameterSets = typeof(CmdletIInvokeDbaXQuery)
            .GetProperty(nameof(CmdletIInvokeDbaXQuery.ReturnType))!
            .GetCustomAttributes(typeof(ParameterAttribute), inherit: false)
            .Cast<ParameterAttribute>()
            .Select(attribute => attribute.ParameterSetName)
            .ToArray();

        Assert.Contains("Query", readerParameterSets);
        Assert.Contains("QueryReader", readerParameterSets);
        Assert.DoesNotContain("QueryReader", streamParameterSets);
        Assert.DoesNotContain("QueryReader", returnTypeParameterSets);
    }

    [Fact]
    public void ExplicitFalseAsDataReader_UsesTheBufferedQueryContract()
    {
        using var table = CreateTable();
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DataTableSqlServer(table);

        try
        {
            var state = InitialSessionState.CreateDefault();
            state.Commands.Add(new SessionStateCmdletEntry(
                "Invoke-DbaXQuery",
                typeof(CmdletIInvokeDbaXQuery),
                helpFileName: null));

            using var powerShell = PowerShell.Create(state);
            powerShell
                .AddCommand("Invoke-DbaXQuery")
                .AddParameter("Server", "localhost")
                .AddParameter("Database", "tempdb")
                .AddParameter("Query", "SELECT 1")
                .AddParameter("ReturnType", ReturnType.DataTable)
                .AddParameter("Stream", false)
                .AddParameter("AsDataReader", false);

            Collection<PSObject> results = powerShell.Invoke();

            var result = Assert.Single(results);
            Assert.IsType<DataTable>(result.BaseObject);
            Assert.Empty(powerShell.Streams.Warning);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    [Fact]
    public void EnabledAsDataReader_RejectsBufferedReturnOptionsBeforeQuerying()
    {
        using var table = CreateTable();
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => new DataTableSqlServer(table);

        try
        {
            var state = InitialSessionState.CreateDefault();
            state.Commands.Add(new SessionStateCmdletEntry(
                "Invoke-DbaXQuery",
                typeof(CmdletIInvokeDbaXQuery),
                helpFileName: null));

            using var powerShell = PowerShell.Create(state);
            powerShell
                .AddCommand("Invoke-DbaXQuery")
                .AddParameter("Server", "localhost")
                .AddParameter("Database", "tempdb")
                .AddParameter("Query", "SELECT 1")
                .AddParameter("ReturnType", ReturnType.DataTable)
                .AddParameter("AsDataReader", true);

            RuntimeException exception = Assert.ThrowsAny<RuntimeException>(() => powerShell.Invoke());

            Assert.Contains("AsDataReader cannot be combined", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
        }
    }

    [Fact]
    public async Task AsDataReader_DisposesReaderWhenPipelineStopsBeforeDelivery()
    {
        using var table = CreateTable();
        var ownedReader = new DbaDataReader(table.CreateDataReader());
        var sqlServer = new DelayedDataReaderSqlServer(ownedReader);
        CmdletIInvokeDbaXQuery.SqlServerFactory = () => sqlServer;

        try
        {
            using var powerShell = CreateReaderPowerShell(
                queryTimeout: 30,
                parameters: new Hashtable());
            var invocation = powerShell.BeginInvoke();
            Assert.True(
                sqlServer.QueryStarted.Wait(TimeSpan.FromSeconds(5)),
                "The reader query did not start in time.");

            var stopTask = Task.Run(powerShell.Stop);
            Assert.True(
                sqlServer.CancellationRequested.Wait(TimeSpan.FromSeconds(5)),
                "The reader query did not observe pipeline cancellation in time.");
            sqlServer.ReleaseReader();

            await stopTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Throws<PipelineStoppedException>(() => powerShell.EndInvoke(invocation));
            Assert.True(
                SpinWait.SpinUntil(() => ownedReader.IsClosed, TimeSpan.FromSeconds(5)),
                "The canceled query reader was not disposed in time.");
        }
        finally
        {
            CmdletIInvokeDbaXQuery.SqlServerFactory = () => new SqlServer();
            sqlServer.QueryStarted.Dispose();
            sqlServer.CancellationRequested.Dispose();
            ownedReader.Dispose();
        }
    }

    private sealed class DataTableSqlServer : SqlServer
    {
        private readonly DataTable _table;

        public DataTableSqlServer(DataTable table)
        {
            _table = table;
        }

        public override object? Query(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
            => ReturnType == ReturnType.DataRow ? _table.Rows[0] : _table;

        public override Task<DbaDataReader> QueryReaderAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
            => throw new InvalidOperationException("The buffered query contract must not request a data reader.");
    }

    private sealed class TimeoutCaptureSqlServer : SqlServer
    {
        private readonly DataTable _table;

        public TimeoutCaptureSqlServer(DataTable table)
        {
            _table = table;
        }

        public int? AppliedTimeout { get; private set; }

        public override object? Query(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new SqlCommand
            {
                CommandTimeout = 37
            };
            ApplyCommandTimeout(command);
            AppliedTimeout = command.CommandTimeout;
            return _table;
        }
    }

    private sealed class DataReaderSqlServer : SqlServer
    {
        private readonly DbaDataReader _reader;

        public DataReaderSqlServer(DbaDataReader reader)
        {
            _reader = reader;
        }

        public int QueryReaderCalls { get; private set; }

        public string ReceivedConnectionString { get; private set; } = string.Empty;

        public IDictionary<string, object?>? ReceivedParameters { get; private set; }

        public CancellationToken ReceivedCancellationToken { get; private set; }

        public override Task<DbaDataReader> QueryReaderAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            QueryReaderCalls++;
            ReceivedConnectionString = connectionString;
            ReceivedParameters = parameters;
            ReceivedCancellationToken = cancellationToken;
            return Task.FromResult(_reader);
        }
    }

    private sealed class DelayedDataReaderSqlServer : SqlServer
    {
        private readonly DbaDataReader _reader;
        private readonly TaskCompletionSource _releaseReader =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public DelayedDataReaderSqlServer(DbaDataReader reader)
        {
            _reader = reader;
        }

        public ManualResetEventSlim QueryStarted { get; } = new();

        public ManualResetEventSlim CancellationRequested { get; } = new();

        public void ReleaseReader()
            => _releaseReader.TrySetResult();

        public override async Task<DbaDataReader> QueryReaderAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            QueryStarted.Set();
            using var cancellationRegistration = cancellationToken.Register(CancellationRequested.Set);
            await _releaseReader.Task.ConfigureAwait(false);
            return _reader;
        }
    }

    private static DataTable CreateTable()
    {
        var table = new DataTable("Rows");
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        table.Rows.Add(2);
        return table;
    }

    private static Collection<PSObject> InvokeQuery(ReturnType returnType, int? queryTimeout = null)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXQuery", typeof(CmdletIInvokeDbaXQuery), helpFileName: null));

        using var powerShell = PowerShell.Create(state);
        powerShell
            .AddCommand("Invoke-DbaXQuery")
            .AddParameter("Server", "localhost")
            .AddParameter("Database", "tempdb")
            .AddParameter("Query", "SELECT 1")
            .AddParameter("ReturnType", returnType);

        if (queryTimeout.HasValue)
        {
            powerShell.AddParameter("QueryTimeout", queryTimeout.Value);
        }

        return powerShell.Invoke();
    }

    private static Collection<PSObject> InvokeReader(int queryTimeout, Hashtable parameters)
    {
        using var powerShell = CreateReaderPowerShell(queryTimeout, parameters);

        return powerShell.Invoke();
    }

    private static PowerShell CreateReaderPowerShell(int queryTimeout, Hashtable parameters)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXQuery", typeof(CmdletIInvokeDbaXQuery), helpFileName: null));

        var powerShell = PowerShell.Create(state);
        powerShell
            .AddCommand("Invoke-DbaXQuery")
            .AddParameter("Server", "localhost")
            .AddParameter("Database", "tempdb")
            .AddParameter("Query", "SELECT Id FROM dbo.Rows WHERE Id >= @MinimumId")
            .AddParameter("QueryTimeout", queryTimeout)
            .AddParameter("Parameters", parameters)
            .AddParameter("TrustServerCertificate")
            .AddParameter("AsDataReader");
        return powerShell;
    }
}
