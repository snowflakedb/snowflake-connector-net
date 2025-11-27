using System;
using Snowflake.Data.Client;
using Microsoft.Extensions.Logging;

namespace MinicoreVerifyApp
{
    public class ProbeLogger : ILogger
    {
        public bool MiniCoreLoaded { get; private set; }
        public string Version { get; private set; }

        public IDisposable BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            var msg = formatter(state, exception);
            // Intercept the specific success message from SfMiniCore.cs
            if (msg != null && msg.Contains("MiniCore loaded successfully"))
            {
                MiniCoreLoaded = true;
                Console.WriteLine($"[PROBE] CAPTURED: {msg}");
                // Extract version if needed
                var parts = msg.Split(new[] { "Version: " }, StringSplitOptions.None);
                if (parts.Length > 1) Version = parts[1];
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("🔍 Starting MiniCore verification...");
            
            var probe = new ProbeLogger();
            // Register our custom logger
            SnowflakeDbLoggerConfig.SetCustomLogger(probe);

            try
            {
                // Create connection to trigger SFEnvironment initialization
                // We don't need a real connection, just enough to trigger the static constructor
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = "account=dummy;user=dummy;password=dummy;host=localhost;scheme=http";
                    Console.WriteLine("🔌 Triggering driver initialization...");
                    // Open() triggers SFEnvironment static ctor -> ClientEnv -> ExtractMinicoreVersion -> SfMiniCore.TryGetVersionSafe -> Log
                    conn.Open(); 
                }
            }
            catch (Exception)
            {
                // Expected connection failure, we just wanted to trigger init
            }

            if (probe.MiniCoreLoaded)
            {
                Console.WriteLine($"✅ SUCCESS: MiniCore loaded from NuGet package! Version: {probe.Version}");
                Environment.Exit(0);
            }
            else
            {
                Console.WriteLine("❌ FAILURE: MiniCore load message not found in logs.");
                Console.WriteLine("This means the native library was not found or failed to load.");
                Environment.Exit(1);
            }
        }
    }
}
