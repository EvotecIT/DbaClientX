#if NET8_0_OR_GREATER
using System;
using System.IO;
using System.Runtime.InteropServices;

namespace DBAClientX;

public partial class SQLite
{
    private static readonly object NativeRuntimeLock = new();
    private static string? _nativeLibraryPath;
    private static nint _nativeLibraryHandle;

    /// <summary>Loads the bundled SQLite native library from an explicit absolute directory.</summary>
    /// <param name="directory">The trusted directory containing the deployed native SQLite library.</param>
    /// <remarks>
    /// Call before opening the first SQLite connection when hosting in a native shared library whose
    /// directory differs from the application's executable directory. The library remains loaded for
    /// the process lifetime because SQLite provider state and callbacks can retain native addresses.
    /// Repeated calls for the same directory are allowed; a different directory is rejected.
    /// This does not alter the process DLL search path or initialize a second SQLite provider.
    /// </remarks>
    public static void InitializeNativeLibraryDirectory(string directory)
    {
        if (string.IsNullOrWhiteSpace(directory) || !Path.IsPathFullyQualified(directory))
            throw new ArgumentException("An absolute native library directory is required.", nameof(directory));
        string name = OperatingSystem.IsWindows() ? "e_sqlite3.dll" :
            OperatingSystem.IsMacOS() ? "libe_sqlite3.dylib" : "libe_sqlite3.so";
        string path = Path.GetFullPath(Path.Combine(directory, name));
        lock (NativeRuntimeLock)
        {
            if (_nativeLibraryPath is not null)
            {
                var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
                if (!string.Equals(_nativeLibraryPath, path, comparison))
                    throw new InvalidOperationException("The SQLite native library directory is already initialized.");
                return;
            }
            _nativeLibraryHandle = NativeLibrary.Load(path);
            _nativeLibraryPath = path;
        }
    }
}
#endif
