using DBAClientX;

SQLite.InitializeNativeLibraryDirectory(AppContext.BaseDirectory);
SQLite.InitializeNativeLibraryDirectory(AppContext.BaseDirectory);
try {
    SQLite.InitializeNativeLibraryDirectory(Path.GetPathRoot(AppContext.BaseDirectory)!);
    throw new InvalidOperationException("Changing the initialized native runtime directory was accepted.");
} catch (InvalidOperationException exception) when (exception.Message.Contains("already initialized", StringComparison.Ordinal)) { }
using var client = new SQLite();
using var reader = await client.QueryReaderAsync(":memory:", "SELECT 42 AS Id, 'Żółć' AS Name");
if (!reader.Read() || reader.GetFieldType(0) != typeof(long) || reader.GetFieldType(1) != typeof(string) ||
    reader.GetInt64(0) != 42 || reader.GetString(1) != "Żółć" || reader.Read())
    throw new InvalidOperationException("Native SQLite reader contract failed.");
Console.WriteLine("Native SQLite reader: field types, typed values, Unicode and lifetime passed.");
