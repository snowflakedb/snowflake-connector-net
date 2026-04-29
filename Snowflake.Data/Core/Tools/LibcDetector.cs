using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal enum LibcFamily
    {
        NotApplicable,
        Glibc,
        Musl,
        CouldNotDetermine
    }

    internal interface ILibcDetector
    {
        (LibcFamily Family, string Version) Detect();
    }

    internal sealed class LibcDetector : ILibcDetector
    {
        private const int LddTimeoutInMs = 2000;
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

            if (TryGetMuslVersionFromLdd(out var muslVersion))
                return (LibcFamily.Musl, muslVersion);

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
                return true;
            }
            catch (Exception e)
            {
                s_logger.Debug($"gnu_get_libc_version not available: {e.Message}!");
                version = null;
                return false;
            }
        }

        internal static bool TryGetMuslVersionFromLdd(out string version)
        {
            try
            {
                using var process = new Process();
                process.StartInfo = new ProcessStartInfo
                {
                    FileName = "ldd",
                    Arguments = "--version",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                var output = new StringBuilder();
                process.OutputDataReceived += (s, e) => output.Append(e.Data);
                process.ErrorDataReceived += (s, e) => output.Append($"err: {e.Data}");
                process.Start();
                process.WaitForExit(LddTimeoutInMs);

                version = ParseMuslVersionFromLddOutput(output.ToString());
                return version != null;
            }
            catch (Exception e)
            {
                s_logger.Debug($"Failed to run ldd --version for musl detection: {e.Message}!");
                version = null;
                return false;
            }
        }


        internal static string ParseMuslVersionFromLddOutput(string output)
        {
            if (string.IsNullOrEmpty(output))
                return null;

            // musl ldd --version output:
            //   musl libc (x86_64)
            //   Version 1.2.5
            if (!output.Contains("musl"))
                return null;

            var line = output.Split('\n').FirstOrDefault(x => x.IndexOf("Version ", StringComparison.OrdinalIgnoreCase) != -1);

            if (string.IsNullOrEmpty(line))
                return null;

            var trimmed = line.Trim();
            if (trimmed.StartsWith("Version ", StringComparison.OrdinalIgnoreCase))
                return trimmed.Substring("Version ".Length).Trim();

            return null;
        }
    }

    internal static class LibcFamilyExtensions
    {

        internal static string ToPrettyString(this LibcFamily family)
        {
            return family switch
            {
                LibcFamily.Glibc => "glibc",
                LibcFamily.Musl => "musl",
                LibcFamily.CouldNotDetermine => "could not determine",
                _ => null
            };
        }
    }
}
