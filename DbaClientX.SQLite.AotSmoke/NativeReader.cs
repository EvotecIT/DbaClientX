using DBAClientX;
using System.Runtime.InteropServices;

internal static class NativeReader
{
    [UnmanagedCallersOnly(EntryPoint = "verify_sqlite_reader")]
    internal static int Verify(nint directory)
    {
        try
        {
            string path = Marshal.PtrToStringUTF8(directory) ?? throw new ArgumentException("Directory is required.");
            SQLite.InitializeNativeLibraryDirectory(path);
            SQLite.InitializeNativeLibraryDirectory(path);
            try
            {
                SQLite.InitializeNativeLibraryDirectory(Path.GetPathRoot(path)!);
                throw new InvalidOperationException("Changing the initialized native runtime directory was accepted.");
            }
            catch (InvalidOperationException exception) when (exception.Message.Contains("already initialized", StringComparison.Ordinal)) { }
            using var client = new SQLite();
            using var reader = client.QueryReaderAsync(":memory:", "SELECT 42 AS Id, 'Żółć' AS Name").GetAwaiter().GetResult();
            if (!reader.Read() || reader.GetFieldType(0) != typeof(long) || reader.GetFieldType(1) != typeof(string) ||
                reader.GetInt64(0) != 42 || reader.GetString(1) != "Żółć" || reader.Read())
                throw new InvalidOperationException("Native SQLite reader contract failed.");
            return 0;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine(exception);
            return 1;
        }
    }
}
