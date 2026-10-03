using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DBAClientX;

/// <summary>Resolves SQLite filesystem aliases for guards without opening a database or changing its contents.</summary>
internal static class SQLiteFilePath
{
    internal static string NormalizeWindowsAlias(string path)
    {
        if (Path.DirectorySeparatorChar != '\\') return path;
        if (path.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) return @"\\" + path.Substring(8);
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal) && path.Length >= 7 &&
            char.IsLetter(path[4]) && path[5] == ':' && (path[6] == '\\' || path[6] == '/')) return path.Substring(4);
        return path;
    }

    /// <summary>Resolves existing files and ancestor directories, retaining a missing destination's suffix.</summary>
    internal static string ResolveAliases(string path)
    {
        string fullPath = Path.GetFullPath(NormalizeWindowsAlias(path));
        string existing = fullPath;
        var suffix = new Stack<string>();
        bool windows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
        while (!File.Exists(windows ? ToExtendedPath(existing) : existing) &&
               !Directory.Exists(windows ? ToExtendedPath(existing) : existing))
        {
            string? parent = Path.GetDirectoryName(existing);
            if (string.IsNullOrEmpty(parent) || string.Equals(parent, existing, StringComparison.Ordinal)) return fullPath;
            suffix.Push(Path.GetFileName(existing));
            existing = parent!;
        }

        string resolved = windows ? ResolveWindows(existing) : ResolveUnix(existing);
        while (suffix.Count != 0) resolved = Path.Combine(resolved, suffix.Pop());
        return NormalizeWindowsAlias(resolved);
    }

    private static string ToExtendedPath(string path)
        => path.StartsWith(@"\\?\", StringComparison.Ordinal) ? path
            : path.StartsWith(@"\\", StringComparison.Ordinal) ? @"\\?\UNC\" + path.Substring(2) : @"\\?\" + path;

    private static string ResolveWindows(string path)
    {
        // Zero desired access queries metadata; read/write/delete sharing does not take a SQLite lock.
        using var handle = CreateFile(ToExtendedPath(path), 0, 7, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero);
        if (handle.IsInvalid) throw ResolutionFailure();
        var buffer = new StringBuilder(512);
        uint length = GetFinalPathNameByHandle(handle, buffer, (uint)buffer.Capacity, 0);
        if (length >= buffer.Capacity)
        {
            buffer.Capacity = checked((int)length + 1);
            length = GetFinalPathNameByHandle(handle, buffer, (uint)buffer.Capacity, 0);
        }
        if (length == 0 || length >= buffer.Capacity) throw ResolutionFailure();
        return buffer.ToString();
    }

    private static string ResolveUnix(string path)
    {
        // POSIX realpath resolves every directory component, including macOS /var -> /private/var.
        IntPtr resolved = RealPath(path, IntPtr.Zero);
        if (resolved == IntPtr.Zero) throw ResolutionFailure();
        try { return Marshal.PtrToStringAnsi(resolved) ?? throw new IOException("Could not resolve SQLite filesystem aliases."); }
        finally { Free(resolved); }
    }

    private static IOException ResolutionFailure()
        => new IOException("Could not resolve SQLite filesystem aliases.", new Win32Exception(Marshal.GetLastWin32Error()));

    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFile(string path, uint desiredAccess, uint shareMode,
        IntPtr securityAttributes, uint creationDisposition, uint flags, IntPtr templateFile);

    [DllImport("kernel32.dll", EntryPoint = "GetFinalPathNameByHandleW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandle(SafeFileHandle handle, StringBuilder path, uint length, uint flags);

    [DllImport("libc", EntryPoint = "realpath", CharSet = CharSet.Ansi, CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
    private static extern IntPtr RealPath(string path, IntPtr resolvedPath);

    [DllImport("libc", EntryPoint = "free", CallingConvention = CallingConvention.Cdecl)]
    private static extern void Free(IntPtr pointer);
}
