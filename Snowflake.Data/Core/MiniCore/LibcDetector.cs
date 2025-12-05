using System;
using System.Runtime.InteropServices;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.MiniCore
{
    internal static class LibcDetector
    {
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<object>();
        private static LibcVariant? s_cachedVariant;
        private static readonly object s_lock = new object();

        internal enum LibcVariant
        {
            Glibc,
            Musl,
            Unsupported
        }

        [DllImport("libc", EntryPoint = "gnu_get_libc_version", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr gnu_get_libc_version();

        public static LibcVariant DetectLibcVariant()
        {
            lock (s_lock)
            {
                if (s_cachedVariant.HasValue)
                {
                    return s_cachedVariant.Value;
                }

                s_cachedVariant = DetectLibcVariantInternal();
                return s_cachedVariant.Value;
            }
        }

        private static LibcVariant DetectLibcVariantInternal()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return LibcVariant.Unsupported;
            }

            try
            {
                IntPtr versionPtr = gnu_get_libc_version();
                if (versionPtr != IntPtr.Zero)
                {
                    string version = Marshal.PtrToStringAnsi(versionPtr);
                    Logger.Debug($"Detected glibc version: {version}");
                    return LibcVariant.Glibc;
                }

                Logger.Debug("gnu_get_libc_version returned null, assuming glibc");
                return LibcVariant.Glibc;
            }
            catch (EntryPointNotFoundException)
            {
                Logger.Debug("gnu_get_libc_version not found, detected musl libc");
                return LibcVariant.Musl;
            }
            catch (DllNotFoundException)
            {
                Logger.Warn("libc not found, defaulting to musl");
                return LibcVariant.Musl;
            }
            catch (Exception ex)
            {
                Logger.Warn($"Error detecting libc variant: {ex.Message}, defaulting to musl");
                return LibcVariant.Musl;
            }
        }

        public static string GetLibcIdentifier()
        {
            var variant = DetectLibcVariant();
            return variant switch
            {
                LibcVariant.Glibc => "glibc",
                LibcVariant.Musl => "musl",
                _ => null
            };
        }
    }
}
