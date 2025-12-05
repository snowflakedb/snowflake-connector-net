using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.MiniCore
{
    internal static class SfMiniCore
    {
        private const string LibraryName = "sf_mini_core";
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<object>();
        private static readonly object s_lock = new object();
        private static Task<LoadResult> s_loadTask;

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr sf_core_full_version();

        internal class LoadResult
        {
            public string Version { get; set; }
            public string Error { get; set; }
        }

        public static void StartLoading()
        {
            lock (s_lock)
            {
                if (s_loadTask == null)
                {
                    s_loadTask = Task.Run(LoadVersionInternal);
                }
            }
        }

        public static bool IsLoaded => s_loadTask?.IsCompleted ?? false;

        public static string TryGetVersionSafe()
        {
            return s_loadTask is { IsCompleted: true } ? s_loadTask.Result.Version : null;
        }

        public static string GetLoadError()
        {
            if (s_loadTask == null)
                return "NOT_STARTED";
            if (!s_loadTask.IsCompleted)
                return "NOT_YET_LOADED";
            return s_loadTask.Result.Error;
        }

        public static string GetExpectedLibraryName()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return "sf_mini_core.dll";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return "libsf_mini_core.dylib";
            return "libsf_mini_core.so";
        }

        public static string GetFullVersion()
        {
            IntPtr versionPtr = sf_core_full_version();
            if (versionPtr == IntPtr.Zero)
            {
                throw new InvalidOperationException("sf_core_full_version returned null");
            }

            int length = 0;
            while (Marshal.ReadByte(versionPtr, length) != 0)
            {
                length++;
            }

            byte[] bytes = new byte[length];
            Marshal.Copy(versionPtr, bytes, 0, length);
            return Encoding.UTF8.GetString(bytes);
        }

        private static LoadResult LoadVersionInternal()
        {
            try
            {
                var version = GetFullVersion();
                Logger.Info($"MiniCore loaded successfully. Version: {version}");
                return new LoadResult { Version = version, Error = null };
            }
            catch (DllNotFoundException ex)
            {
                Logger.Error($"MiniCore library not found. Error: {ex.Message}");
                return new LoadResult { Version = null, Error = $"LIBRARY NOT FOUND: {ex.Message}" };
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to load MiniCore: {ex.Message}", ex);
                return new LoadResult { Version = null, Error = $"ERROR: {ex.Message}" };
            }
        }
    }
}
