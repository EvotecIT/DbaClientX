using DBAClientX;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DbaClientX.Tests;

// Local opt-in validation owns these generated names and files even when an operation has an uncertain outcome.
internal sealed class SqlServerRecoveryTestScope : IAsyncDisposable
{
    private readonly string _connectionString;
    private string[] _sourceFiles = Array.Empty<string>();
    private string[] _restoreFiles = Array.Empty<string>();
    private bool _sourceAttempted;
    private static readonly StringComparer PathComparer = OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

    public SqlServerRecoveryTestScope(string connectionString, string backupParent)
    {
        _connectionString = new SqlConnectionStringBuilder(connectionString)
        { InitialCatalog = "master", Enlist = false, Pooling = false, ConnectTimeout = 10 }.ConnectionString;
        SourceName = "DbaClientXBackupTest_" + Guid.NewGuid().ToString("N");
        RestoreName = "DbaClientXRestoreTest_" + Guid.NewGuid().ToString("N");
        BackupDirectory = Path.Combine(backupParent, "DbaClientXRecovery_" + Guid.NewGuid().ToString("N"));
        Assert.False(Directory.Exists(BackupDirectory));
        Directory.CreateDirectory(BackupDirectory);
    }

    public string SourceName { get; }
    public string RestoreName { get; }
    public string BackupDirectory { get; }

    public async Task CreateSourceAsync(SqlConnection connection)
    {
        Assert.False(await DatabaseExistsAsync(connection, SourceName));
        using (var locations = new SqlCommand(
            "SELECT CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath')), "
            + "CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultLogPath'))", connection))
        using (var reader = await locations.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            _sourceFiles = new[] { Path.Combine(reader.GetString(0), SourceName + ".mdf"),
                Path.Combine(reader.GetString(1), SourceName + "_log.ldf") };
        }
        Assert.All(_sourceFiles, path => Assert.False(File.Exists(path)));
        _sourceAttempted = true;
        using var create = new SqlCommand("CREATE DATABASE [" + SourceName + "]", connection);
        await create.ExecuteNonQueryAsync();
        _sourceFiles = await ReadDatabaseFilesAsync(connection, SourceName);
    }

    public void RecordRestoreFiles(IEnumerable<string> paths)
    {
        var snapshot = paths.ToArray();
        Assert.NotEmpty(snapshot);
        Assert.All(snapshot, path => Assert.False(File.Exists(path)));
        _restoreFiles = snapshot;
    }

    public void RecordAdditionalSourceFile(string path)
    {
        Assert.True(_sourceAttempted);
        Assert.False(File.Exists(path));
        Assert.Equal(Path.GetFullPath(BackupDirectory), Path.GetDirectoryName(Path.GetFullPath(path)));
        _sourceFiles = _sourceFiles.Append(path).ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        await RunCleanupAsync(new Func<Task>[]
        {
            () => DropOwnedDatabaseAsync(RestoreName, _restoreFiles),
            () => _sourceAttempted ? DropOwnedDatabaseAsync(SourceName, _sourceFiles) : Task.CompletedTask,
            () => DeleteDatabaseFilesAsync(RestoreName, _restoreFiles),
            () => _sourceAttempted ? DeleteDatabaseFilesAsync(SourceName, _sourceFiles) : Task.CompletedTask,
            DeleteBackupDirectoryAsync
        });
    }

    internal static async Task RunCleanupAsync(IEnumerable<Func<Task>> actions)
    {
        var errors = new List<Exception>();
        foreach (var action in actions)
        {
            try { await action(); }
            catch (Exception exception) { errors.Add(exception); }
        }
        if (errors.Count != 0) throw new AggregateException("Recovery test cleanup did not complete.", errors);
    }

    private async Task DropOwnedDatabaseAsync(string name, IReadOnlyList<string> expectedFiles)
    {
        if (expectedFiles.Count == 0) return;
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        if (!await DatabaseExistsAsync(connection, name)) return;
        string[] currentFiles = await ReadDatabaseFilesAsync(connection, name);
        var expected = new HashSet<string>(expectedFiles.Select(Path.GetFullPath), PathComparer);
        if (currentFiles.Length == 0 || currentFiles.Any(path => !expected.Contains(Path.GetFullPath(path))))
            throw new InvalidOperationException("The temporary database's files do not match the test-owned files; cleanup was refused.");
        // SQL Server leaves files behind when a RESTORING database is dropped. Complete only this
        // test-owned full restore first so native DROP removes service-owned files without ACL changes.
        using var state = new SqlCommand("SELECT state_desc FROM sys.databases WHERE name = @name", connection);
        state.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = name;
        if (string.Equals(await state.ExecuteScalarAsync() as string, "RESTORING", StringComparison.Ordinal))
        {
            using var recover = new SqlCommand("RESTORE DATABASE [" + name + "] WITH RECOVERY", connection) { CommandTimeout = 30 };
            try { await recover.ExecuteNonQueryAsync(); }
            catch (SqlException exception) when (exception.Number == 4333)
            {
                // A restore interrupted before its log was restored cannot recover. DROP still
                // removes this verified owned registration; the absent-name guard below then
                // permits deletion of its recorded files through the local fixture directory.
            }
        }
        using var drop = new SqlCommand("DROP DATABASE [" + name + "]", connection) { CommandTimeout = 30 };
        await drop.ExecuteNonQueryAsync();
    }

    private async Task DeleteDatabaseFilesAsync(string name, IReadOnlyList<string> paths)
    {
        if (paths.Count == 0) return;
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        if (await DatabaseExistsAsync(connection, name))
            throw new InvalidOperationException("Temporary database files were retained because their database still exists.");
        // A failed restore can leave unregistered files. Only paths absent before this scope's attempt are eligible.
        await RunCleanupAsync(paths.Select<string, Func<Task>>(path => () =>
        {
            if (File.Exists(path)) File.Delete(path);
            return Task.CompletedTask;
        }));
    }

    private async Task DeleteBackupDirectoryAsync()
    {
        if (!Directory.Exists(BackupDirectory)) return;
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        if (await DatabaseExistsAsync(connection, RestoreName) || await DatabaseExistsAsync(connection, SourceName))
            throw new InvalidOperationException("Temporary backup directory was retained because a test database still exists.");
        if ((File.GetAttributes(BackupDirectory) & FileAttributes.ReparsePoint) != 0)
            throw new InvalidOperationException("Cleanup refused a reparse-point backup directory.");
        var actions = Directory.GetFiles(BackupDirectory).Select<string, Func<Task>>(path => () =>
        {
            if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
                throw new InvalidOperationException("Cleanup refused a reparse-point backup file.");
            File.Delete(path);
            return Task.CompletedTask;
        }).ToList();
        actions.Add(() => { Directory.Delete(BackupDirectory, recursive: false); return Task.CompletedTask; });
        await RunCleanupAsync(actions);
    }

    internal static async Task<bool> DatabaseExistsAsync(SqlConnection connection, string name)
    {
        using var command = new SqlCommand("SELECT DB_ID(@name)", connection);
        command.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = name;
        return await command.ExecuteScalarAsync() is not DBNull;
    }

    private static async Task<string[]> ReadDatabaseFilesAsync(SqlConnection connection, string name)
    {
        using var command = new SqlCommand("SELECT physical_name FROM sys.master_files WHERE database_id = DB_ID(@name)", connection);
        command.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = name;
        using var reader = await command.ExecuteReaderAsync();
        var paths = new List<string>();
        while (await reader.ReadAsync()) paths.Add(reader.GetString(0));
        return paths.ToArray();
    }
}
