using System;
using System.Runtime.InteropServices;
using System.Text;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.MiniCore
{
    internal static class SfMiniCore
    {
        private const string LibraryName = "sf_mini_core";
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<object>();

        private static readonly Lazy<string> s_cachedVersion = new Lazy<string>(LoadVersionInternal);

        [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr sf_core_full_version();

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

        public static string TryGetVersionSafe()
        {
            return s_cachedVersion.Value;
        }

        private static string LoadVersionInternal()
        {
            try
            {
                var version = GetFullVersion();
                Logger.Info($"MiniCore loaded successfully. Version: {version}");
                return version;
            }
            catch (DllNotFoundException ex)
            {
                Logger.Error($"MiniCore library not found. Error: {ex.Message}");
                return $"LIBRARY NOT FOUND ERROR: {ex.Message}";
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to load MiniCore: {ex.Message}", ex);
                return $"ERROR: {ex.Message}";
            }
        }
    }
}

