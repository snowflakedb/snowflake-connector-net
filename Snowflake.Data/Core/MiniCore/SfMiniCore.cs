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
        internal const string DISABLED_MESSAGE = "Minicore is disabled with SNOWFLAKE_DISABLE_MINICORE env variable";
        internal const string FAILED_TO_LOAD_MESSAGE = "Failed to load binary";
        internal const string STILL_LOADING_MESSAGE = "Minicore is still loading";

        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<object>();
        private static readonly object s_lock = new object();
        private static Task<LoadResult> s_loadTask;

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr sf_core_full_version();

        internal class LoadResult
        {
            public string Version { get; set; }
            public string Logs { get; set; }
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
            if (s_loadTask == null || !s_loadTask.IsCompleted)
                return STILL_LOADING_MESSAGE;
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
            var logs = new StringBuilder();
            try
            {
                var version = GetFullVersion();
                logs.Append($"MiniCore loaded successfully. Version: {version}");
                Logger.Debug(logs.ToString());
                return new LoadResult { Version = version, Logs = logs.ToString(), Error = null };
            }
            catch (DllNotFoundException ex)
            {
                logs.Append($"MiniCore library not found. Error: {ex.Message}");
                var maskedLogs = SecretDetector.MaskSecrets(logs.ToString()).maskedText;
                Logger.Debug(maskedLogs);
                return new LoadResult { Version = null, Logs = maskedLogs, Error = FAILED_TO_LOAD_MESSAGE };
            }
            catch (Exception ex)
            {
                logs.Append($"Failed to load MiniCore: {ex.Message}");
                var maskedLogs = SecretDetector.MaskSecrets(logs.ToString()).maskedText;
                Logger.Debug(maskedLogs, ex);
                return new LoadResult { Version = null, Logs = maskedLogs, Error = FAILED_TO_LOAD_MESSAGE };
            }
        }
    }
}
