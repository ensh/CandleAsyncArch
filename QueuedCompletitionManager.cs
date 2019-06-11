using System;
using System.Runtime.InteropServices;
using System.Threading;
using Core.Classes;
using Microsoft.Win32.SafeHandles;

namespace Core
{
    public class QueuedCompletitionManager
    {
        internal static class NativeMethods
        {
            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
            internal static extern IntPtr CreateIoCompletionPort(SafeFileHandle handle, IntPtr port, uint CompletionKey, int NumberOfConcurrentThreads);

            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
            internal static extern bool GetQueuedCompletionStatus(IntPtr CompletionPort, out uint lpNumberOfBytes, out IntPtr lpCompletionKey, out IntPtr lpOverlapped, int dwMilliseconds);

            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
            internal static extern bool PostQueuedCompletionStatus(IntPtr CompletionPort, uint lpNumberOfBytes, IntPtr lpCompletionKey, IntPtr lpOverlapped);
        }

        public QueuedCompletitionManager() : this(1) { }

        public QueuedCompletitionManager(int count)
        {
            count = Math.Max(count, 1);
            _completionPortHandle = NativeMethods.CreateIoCompletionPort(new SafeFileHandle(new IntPtr(-1), false), IntPtr.Zero, 0, count);
            _completionPortThreads = new Thread[count];
        }

        IntPtr _completionPortHandle;
        Thread[] _completionPortThreads;

        public virtual void Start()
        {
            if (_completionPortThreads[0] == null)
            {
                for (int i = 0; i < _completionPortThreads.Length; i++)
                {
				    _completionPortThreads[i] = CoreHelper.GetThread(() => Execute(_completionPortHandle), true, GetType().Name + "_" + i.ToString());
                    // new Thread(() => Execute(_completionPortHandle)) { IsBackground = true, Name = GetType().Name + "_"+ i.ToString() };
                    _completionPortThreads[i].Start();
                }
            }
        }
        public virtual void Stop()
        {
            if (_completionPortThreads[0] != null)
            {
                _terminate = true;
                for (int i = 0; i < _completionPortThreads.Length; i++)
                    Post();
            }
        }
        protected virtual int Delay
        {
            get
            {
                return int.MaxValue;
            }
        }
        protected virtual void Apply()
        {
        }
        protected virtual void Timeout()
        {
        }
        volatile bool _terminate;
        void Execute(IntPtr completionPort)
        {
            while (!_terminate)
            {
                uint bytesRead = 0;
                IntPtr completionKey;
                IntPtr nativeOverlapped;

                int delay = Delay;

                if (delay != 0)
                {
                    if (NativeMethods.GetQueuedCompletionStatus(completionPort, out bytesRead, out completionKey, 
                        out nativeOverlapped, delay)) Apply();
                    else
                        Timeout();
                }
                else
                    Timeout();
            }
        }

        public virtual void Post()
        {
            NativeMethods.PostQueuedCompletionStatus(_completionPortHandle, 0, IntPtr.Zero, IntPtr.Zero);
        }
    }
}
