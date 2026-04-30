using System;
using System.Runtime.InteropServices;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    // Musl is not supported: the driver depends on Mono.Posix which has no musl-compatible native binary.
    internal enum LibcFamily
    {
        NotApplicable,
        Glibc,
        CouldNotDetermine
    }

    internal interface ILibcDetector
    {
        (LibcFamily Family, string Version) Detect();
    }

    internal sealed class LibcDetector : ILibcDetector
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<LibcDetector>();
        internal static ILibcDetector Instance { get; } = new LibcDetector();

        [DllImport("libc", EntryPoint = "gnu_get_libc_version", CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr GnuGetLibcVersion();

        private LibcDetector() {}

        public (LibcFamily Family, string Version) Detect()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return (LibcFamily.NotApplicable, null);

            if (TryGetGlibcVersion(out var glibcVersion))
                return (LibcFamily.Glibc, glibcVersion);

            return (LibcFamily.CouldNotDetermine, null);
        }

        internal static bool TryGetGlibcVersion(out string version)
        {
            try
            {
                var ptr = GnuGetLibcVersion();
                if (ptr == IntPtr.Zero)
                {
                    version = null;
                    return false;
                }
                version = Marshal.PtrToStringAnsi(ptr);
                return !string.IsNullOrEmpty(version);
            }
            catch (Exception e)
            {
                s_logger.Debug($"gnu_get_libc_version not available: {e.Message}!");
                version = null;
                return false;
            }
        }
    }

    internal static class LibcFamilyExtensions
    {
        internal static string ToPrettyString(this LibcFamily family)
        {
            return family switch
            {
                LibcFamily.Glibc => "glibc",
                LibcFamily.CouldNotDetermine => "could not determine",
                _ => null
            };
        }
    }
}
