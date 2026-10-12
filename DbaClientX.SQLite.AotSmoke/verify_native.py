"""Exercise SQLite in a NativeAOT library from a separate Python host directory."""
import ctypes
import os
from pathlib import Path
import sys
import tempfile

library_path = Path(sys.argv[1]).resolve(strict=True)
# Keep both the executable and working directory away from the native assets.
with tempfile.TemporaryDirectory(prefix="dbax-native-host-") as working_directory:
    original_directory = Path.cwd()
    try:
        os.chdir(working_directory)
        library = ctypes.CDLL(str(library_path))
        verify = library.verify_sqlite_reader
        verify.argtypes = [ctypes.c_char_p]
        verify.restype = ctypes.c_int
        if verify(str(library_path.parent).encode("utf-8")) != 0:
            raise RuntimeError("Separate-host native SQLite reader verification failed.")
    finally:
        os.chdir(original_directory)
print("Separate-host SQLite: native binding, field types, Unicode and lifetime passed.")
