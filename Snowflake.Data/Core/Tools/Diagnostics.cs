using System;
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Text;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class Diagnostics
    {
        private const int PadRight = -25;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<Diagnostics>();

        public static void LogDiagnostics() => s_logger.Info(GetDiagnosticInfo());

        private static string GetDiagnosticInfo()
        {
            StringBuilder info = new StringBuilder("System Diagnostics:\n");
            try
            {
                info.AppendLine($"{"OS",PadRight}: {OsName()}");
                info.AppendLine($"{"OS Description",PadRight}: {RuntimeInformation.OSDescription}");
                info.AppendLine($"{"OS Architecture",PadRight}: {RuntimeInformation.OSArchitecture}");
                info.AppendLine($"{"OS Version",PadRight}: {Environment.OSVersion}");
                info.AppendLine($"{"OS x64",PadRight}: {Environment.Is64BitOperatingSystem}");
                info.AppendLine($"{"Processor Architecture",PadRight}: {RuntimeInformation.ProcessArchitecture}");
                info.AppendLine($"{"Processor Count",PadRight}: {Environment.ProcessorCount}");
                info.AppendLine($"{".NET Framework",PadRight}: {RuntimeInformation.FrameworkDescription}");
                info.AppendLine($"{"CLR Runtime Version",PadRight}: {Environment.Version}");
                info.AppendLine($"{"App x64",PadRight}: {Environment.Is64BitProcess}");
                info.AppendLine($"{"GC Server Mode",PadRight}: {GCSettings.IsServerGC}");
                info.AppendLine($"{"GC LOH Compaction Mode",PadRight}: {GCSettings.LargeObjectHeapCompactionMode}");
                info.AppendLine($"{"GC Latency Mode",PadRight}: {GCSettings.LatencyMode}");
                info.AppendLine($"{"GC Total Memory",PadRight}: {GC.GetTotalMemory(false)}");
                AppendAssemblyInfo(info, Assembly.GetEntryAssembly(), "App");
                AppendAssemblyInfo(info, Assembly.GetExecutingAssembly(), "Driver");
            }
            catch (Exception exception)
            {
                var errorMessage = $"Error caught while collecting diagnostic info: {exception.Message}";
                s_logger.Error(errorMessage, exception);
                info.AppendLine(errorMessage);
            }
            return info.ToString();
        }

        private static void AppendAssemblyInfo(StringBuilder info, Assembly assembly, string assemblyTag)
        {
            if (assembly != null)
            {
                info.AppendLine($"{assemblyTag + "Name",PadRight}: {assembly.GetName().Name}");
                info.AppendLine($"{assemblyTag + "Version",PadRight}: {assembly.GetName().Version}");
            }
        }

        private static string OsName()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return "LINUX";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return "WINDOWS";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return "OSX";
            return "Unknown";
        }
    }
}
