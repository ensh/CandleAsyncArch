using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Permissions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace AD.Common.Helpers
{
    internal unsafe static class NativeMethods
    {
        private const string KERNEL32 = "kernel32.dll";

        [Flags]
        internal enum EFileAttributes : uint
        {
            Readonly = 0x00000001,
            Hidden = 0x00000002,
            System = 0x00000004,
            Directory = 0x00000010,
            Archive = 0x00000020,
            Device = 0x00000040,
            Normal = 0x00000080,
            Temporary = 0x00000100,
            SparseFile = 0x00000200,
            ReparsePoint = 0x00000400,
            Compressed = 0x00000800,
            Offline = 0x00001000,
            NotContentIndexed = 0x00002000,
            Encrypted = 0x00004000,
            WriteThrough = 0x80000000,
            Overlapped = 0x40000000,
            NoBuffering = 0x20000000,
            RandomAccess = 0x10000000,
            SequentialScan = 0x08000000,
            DeleteOnClose = 0x04000000,
            BackupSemantics = 0x02000000,
            PosixSemantics = 0x01000000,
            OpenReparsePoint = 0x00200000,
            OpenNoRecall = 0x00100000,
            FirstPipeInstance = 0x00080000
        }

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true, CharSet = CharSet.Auto, BestFitMapping = false), SecurityCritical]
        internal static unsafe extern IntPtr CreateFile(
            string lpFileName,
            [MarshalAs(UnmanagedType.U4)] FileAccess dwDesiredAccess,
            [MarshalAs(UnmanagedType.U4)] FileShare dwShareMode,
            IntPtr lpSecurityAttributes,
            [MarshalAs(UnmanagedType.U4)] FileMode dwCreationDisposition,
            [MarshalAs(UnmanagedType.U4)] EFileAttributes dwFlagsAndAttributes,
            IntPtr hTemplateFile);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static unsafe extern int WriteFile(IntPtr hFile, byte* lpBuffer, int nNumberOfBytesToWrite, IntPtr lpNumberOfBytesWritten, NativeOverlapped* lpOverlapped);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static unsafe extern int ReadFile(IntPtr hFile, byte* lpBuffer, int nNumberOfBytesToRead, IntPtr lpNumberOfBytesRead, NativeOverlapped* lpOverlapped);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true, EntryPoint = "SetFilePointer"), SecurityCritical]
        private static unsafe extern int SetFilePointerWin32(IntPtr handle, int lo, int* hi, int origin);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static unsafe extern bool FlushFileBuffers(IntPtr handle);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static extern bool CancelIo(IntPtr handle);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static extern bool LockFile(IntPtr hFile, int dwFileOffsetLow, int dwFileOffsetHigh, int nNumberOfBytesToLockLow, int nNumberOfBytesToLockHigh);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static extern bool UnlockFile(IntPtr hFile, int dwFileOffsetLow, int dwFileOffsetHigh, int nNumberOfBytesToLockLow, int nNumberOfBytesToLockHigh);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static unsafe extern bool CloseHandle(IntPtr handle);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static unsafe extern uint GetLastError();

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true), SecurityCritical]
        internal static unsafe extern bool GetOverlappedResult(IntPtr handle, NativeOverlapped* lpOverlapped, IntPtr lpNumberOfBytesTransferred, bool bWait);

        [SuppressUnmanagedCodeSecurityAttribute, DllImport(KERNEL32, SetLastError = true, EntryPoint = "RtlZeroMemory"), SecurityCritical]
        internal static extern unsafe void ZeroMemory(void* dst, int length);

        [SuppressUnmanagedCodeSecurityAttribute, SecuritySafeCritical]
        internal static unsafe long SetFilePointer(IntPtr handle, long offset, SeekOrigin origin, out int hr)
        {
            hr = 0;
            int lo = (int)offset;
            int hi = (int)(offset >> 32);
            lo = SetFilePointerWin32(handle, lo, &hi, (int)origin);
            if (lo == -1 && ((hr = Marshal.GetLastWin32Error()) != 0))
                return -1;
            return (long)(((ulong)((uint)hi)) << 32) | ((uint)lo);
        }
    }

    public sealed class AsyncLogFile : IDisposable
    {
        private IntPtr _fileHandle;
        private int _offset;

        public AsyncLogFile(string fileName, bool createImmediate = true)
        {
            new FileIOPermission(FileIOPermissionAccess.Write | FileIOPermissionAccess.Read, new String[] { fileName }).Demand();
            if (createImmediate)
                Create(fileName);
        }

        public long Offset { get { return _offset; } }

        [SecuritySafeCritical]
        public void Create(string fileName)
        {
            _fileHandle = NativeMethods.CreateFile(fileName, FileAccess.Write | FileAccess.Read,
                FileShare.ReadWrite, IntPtr.Zero, FileMode.OpenOrCreate,
                NativeMethods.EFileAttributes.Overlapped, IntPtr.Zero);
            _offset = (int)NativeMethods.SetFilePointer(_fileHandle, 0, SeekOrigin.End, out var hr);

            ThreadPool.BindHandle(_fileHandle);
        }

        ~AsyncLogFile()
        {
            Dispose(false);
        }

        private static int OverlappedSize = Marshal.SizeOf(typeof(NativeOverlapped));

        private volatile int lastPosition;
        private const int DefaultFileFlushSize = 4096;

        [SecurityCritical]
        public unsafe void WriteComplete(uint errorCode, uint numBytes, NativeOverlapped* pOverlapped)
        {
            if (_fileHandle != IntPtr.Zero)
            {
                int temp = lastPosition;
                if (pOverlapped->OffsetLow - temp > DefaultFileFlushSize)
                {
                    lastPosition = pOverlapped->OffsetLow;
                    NativeMethods.FlushFileBuffers(_fileHandle);
                }
            }

            Overlapped.Free(pOverlapped);
        }

        [SecurityCritical]
        public unsafe void ReadComplete(uint errorCode, uint numBytes, NativeOverlapped* pOverlapped)
        {
            Overlapped.Free(pOverlapped);
        }

        [SecuritySafeCritical]
        public unsafe void Reserve(byte[] buffer, Func<int, bool> checkOffset)
        {
            int temp = 0, offset;
            do
            {
                temp = _offset;
                offset = temp + buffer.Length;
            } while (Interlocked.CompareExchange(ref _offset, offset, temp) != temp);

            checkOffset((int)_offset);
        }

        [SecuritySafeCritical]
        public unsafe void WriteSync(byte[] buffer, Func<int, bool> checkOffset, bool withLock = true)
        {
            int temp = 0, offset;
            do
            {
                temp = _offset;
                offset = temp + buffer.Length;
            } while (Interlocked.CompareExchange(ref _offset, offset, temp) != temp);

            if (checkOffset((int)_offset))
            {
                Overlapped ovl = new Overlapped(temp, 0, IntPtr.Zero, null);
                NativeOverlapped* pOverlapped = ovl.Pack(WriteComplete, buffer);

                try
                {
                    if (withLock)
                    {
                        while (!(withLock = NativeMethods.LockFile(_fileHandle, offset, 0, buffer.Length, 0)))
                            Thread.Sleep(0);
                    }

                    fixed (byte* bytes = buffer)
                    {
                        if (0 == NativeMethods.WriteFile(_fileHandle, bytes, buffer.Length, IntPtr.Zero, pOverlapped))
                        {
                            var dwResult = NativeMethods.GetLastError();
                            if (ERROR_IO_PENDING == dwResult)
                            {
                                IntPtr transferred = Marshal.AllocHGlobal(sizeof(ulong));
                                try
                                {
                                    NativeMethods.GetOverlappedResult(_fileHandle, pOverlapped, transferred, true);
                                }
                                finally
                                {
                                    Marshal.FreeHGlobal(transferred);
                                }
                            }
                        }
                    }
                }
                finally
                {
                    if (withLock)
                        NativeMethods.UnlockFile(_fileHandle, offset, 0, buffer.Length, 0);
                }
            }
        }

        [SecuritySafeCritical]
        public unsafe void Write(byte[] buffer, Func<int, bool> checkOffset)
        {
            int temp = 0, offset;
            do
            {
                temp = _offset;
                offset = temp + buffer.Length;
            } while (Interlocked.CompareExchange(ref _offset, offset, temp) != temp);

            if (checkOffset((int)_offset))
            {
                Overlapped ovl = new Overlapped(temp, 0, IntPtr.Zero, null);
                NativeOverlapped* pOverlapped = ovl.Pack(WriteComplete, buffer);

                fixed (byte* bytes = buffer)
                {
                    NativeMethods.WriteFile(_fileHandle, bytes, buffer.Length, IntPtr.Zero, pOverlapped);
                }
            }
        }

        [SecuritySafeCritical]
        public unsafe void Write(byte[] buffer, int offset)
        {
            Overlapped ovl = new Overlapped(offset, 0, IntPtr.Zero, null);
            NativeOverlapped* pOverlapped = ovl.Pack(WriteComplete, buffer);

            fixed (byte* bytes = buffer)
            {
                NativeMethods.WriteFile(_fileHandle, bytes, buffer.Length, IntPtr.Zero, pOverlapped);
            }
        }

        [SecuritySafeCritical]
        public unsafe void WriteSync(byte[] buffer, int offset, bool withLock = true)
        {
            Overlapped ovl = new Overlapped(offset, 0, IntPtr.Zero, null);
            NativeOverlapped* pOverlapped = ovl.Pack(WriteComplete, buffer);

            try
            {
                if (withLock)
                {
                    while (!(withLock = NativeMethods.LockFile(_fileHandle, offset, 0, buffer.Length, 0)))
                        Thread.Sleep(0);
                }

                fixed (byte* bytes = buffer)
                {
                    if (0 == NativeMethods.WriteFile(_fileHandle, bytes, buffer.Length, IntPtr.Zero, pOverlapped))
                    {
                        var dwResult = NativeMethods.GetLastError();
                        if (ERROR_IO_PENDING == dwResult)
                        {
                            IntPtr transferred = Marshal.AllocHGlobal(sizeof(ulong));
                            try
                            {
                                NativeMethods.GetOverlappedResult(_fileHandle, pOverlapped, transferred, true);
                            }
                            finally
                            {
                                Marshal.FreeHGlobal(transferred);
                            }
                        }
                    }
                }
            }
            finally
            {
                if (withLock)
                    NativeMethods.UnlockFile(_fileHandle, offset, 0, buffer.Length, 0);
            }
        }

        private const int ERROR_HANDLE_EOF = 38;
        private const int ERROR_INVALID_PARAMETER = 87;
        private const int ERROR_IO_PENDING = 997;

        [SecuritySafeCritical]
        public unsafe void Read(byte[] buffer, int offset, bool withLock = true)
        {
            Overlapped ovl = new Overlapped(offset, 0, IntPtr.Zero, null);
            NativeOverlapped* pOverlapped = ovl.Pack(ReadComplete, buffer);

            try
            {
                if (withLock)
                {
                    while (!(withLock = NativeMethods.LockFile(_fileHandle, offset, 0, buffer.Length, 0)))
                        Thread.Sleep(0);
                }

                fixed (byte* bytes = buffer)
                {
                    if (0 == NativeMethods.ReadFile(_fileHandle, bytes, buffer.Length, IntPtr.Zero, pOverlapped))
                    {
                        var dwResult = NativeMethods.GetLastError();
                        if (ERROR_IO_PENDING == dwResult)
                        {
                            IntPtr transferred = Marshal.AllocHGlobal(sizeof(ulong));
                            try
                            {
                                NativeMethods.GetOverlappedResult(_fileHandle, pOverlapped, transferred, true);
                            }
                            finally
                            {
                                Marshal.FreeHGlobal(transferred);
                            }
                        }
                    }
                }
            }
            finally
            {
                if (withLock)
                    NativeMethods.UnlockFile(_fileHandle, offset, 0, buffer.Length, 0);
            }
        }

        [SecuritySafeCritical]
        public void Write(byte[] buffer)
        {
            Write(buffer, _ => true);
        }

        [SecuritySafeCritical]
        public void Flush()
        {
            NativeMethods.FlushFileBuffers(_fileHandle);
        }

        public static string GetCurrentFileName(string fileName, string folderPath)
        {
            uint part = 0;
            if (!Directory.Exists(folderPath))
                Directory.CreateDirectory(folderPath);

            var maxPart = Directory.GetFiles(folderPath, fileName + ".???.log").Max();
            if (maxPart == null)
            {
                if (File.Exists(folderPath + fileName + ".log"))
                {
                    part++;
                }
            }
            else
            {
                maxPart = maxPart.Substring(folderPath.Length + fileName.Length + 1, 3);
                if (uint.TryParse(maxPart, out part))
                {
                    part++;
                }
            }

            return GenerateFilePath(fileName, folderPath, part);
        }

        private static string GenerateFilePath(string fileName, string folderPath, uint part)
        {
            string result;
            // проверка на абсолютный путь
            if (Path.IsPathRooted(fileName))
            {
                result = fileName;
            }
            else
            {
                result = Path.GetFullPath(folderPath + fileName);
            }
            // именование продолжений файла после достижения лимита размера
            result += string.Format(".{0:000}.log", part);
            return result;
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        [SecuritySafeCritical]
        private void Dispose(bool disposing)
        {
            if (_fileHandle != IntPtr.Zero)
            {
                if (disposing)
                {
                    if (_fileHandle != IntPtr.Zero)
                        NativeMethods.FlushFileBuffers(_fileHandle);
                }
                NativeMethods.CloseHandle(_fileHandle);
                _fileHandle = IntPtr.Zero;
            }
        }
        #endregion

    }
}
