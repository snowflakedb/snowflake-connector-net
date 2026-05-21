using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit.Sdk;

#if !NET8_0_OR_GREATER
using Xunit.Abstractions;
#endif

namespace Snowflake.Data.Tests.Util;

public sealed class TestPerformanceRecorder : IDisposable
{
    private readonly Queue<LogEntry> _entries = new();
    private readonly object _lock = new();
    private static readonly string s_filePath;

    static TestPerformanceRecorder()
    {
        var resultText = "test;time_in_ms\n";
        var dotnetVersion = Environment.GetEnvironmentVariable("net_version");
        var cloudEnv = Environment.GetEnvironmentVariable("snowflake_cloud_env");

        var separator = Path.DirectorySeparatorChar;

        // We have to go up 3 times as the working directory path looks as follows:
        // Snowflake.Data.Tests/bin/debug/{.net_version}/
        s_filePath = $"..{separator}..{separator}..{separator}{GetOs()}_{dotnetVersion}_{cloudEnv}_performance.csv";
        File.WriteAllText(s_filePath, resultText);
    }

    public void Dispose()
    {
        LogEntry[] toWrite;
        lock (_lock)
        {
            toWrite = _entries.ToArray();
            _entries.Clear();
        }

        WriteToFile(toWrite).GetAwaiter().GetResult();
    }

    public Task AddEntry(ITestResultMessage testResult)
    {
        LogEntry[] toWrite;
        lock (_lock)
        {
            _entries.Enqueue(new LogEntry
            {
#if NET8_0_OR_GREATER
                TestName = testResult.TestUniqueID,
#else
                TestName = testResult.Test.DisplayName,
#endif
                TestDuration = testResult.ExecutionTime
            });

            if (_entries.Count < 100)
                return Task.CompletedTask;

            toWrite = _entries.ToArray();
            _entries.Clear();
        }

        return WriteToFile(toWrite);
    }

    private static async Task WriteToFile(LogEntry[] entry)
    {
        var entriesStr = entry.Select(x => $"{x.TestName};{x.TestDuration}");
        var entyStr = string.Join("\n", entriesStr);

#if NETFRAMEWORK
        var sw = File.AppendText(s_filePath);
        sw.Write(entyStr);
        sw.Flush();
        sw.Close();
        return;
#else
        await File.AppendAllTextAsync(s_filePath, entyStr);
#endif
    }

    private static string GetOs()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return "windows";

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return "linux";

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            return "macos";

        return "unknown";
    }

    private struct LogEntry
    {
        public string TestName { get; set; }
        public decimal TestDuration { get; set; }
    }
}
