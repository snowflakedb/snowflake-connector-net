using System;
using Snowflake.Data.Client;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace MinicoreVerifyApp
{
    public class ProbeLogger : ILogger
    {
        public bool MiniCoreLoaded { get; private set; }

        private class NullScope : IDisposable
        {
            public static NullScope Instance { get; } = new NullScope();
            public void Dispose() { }
        }

        public IDisposable BeginScope<TState>(TState state) => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            var msg = formatter(state, exception);
            if (msg?.Contains("MiniCore loaded successfully") == true)
            {
                MiniCoreLoaded = true;
                Console.WriteLine($"[PROBE] {msg}");
            }
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var probe = new ProbeLogger();
            SnowflakeDbLoggerConfig.SetCustomLogger(probe);

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = "account=dummy;user=dummy;password=dummy;host=localhost;scheme=http;connection_timeout=5";

            try
            {
                var openTask = conn.OpenAsync();
                var completed = await Task.WhenAny(openTask, Task.Delay(TimeSpan.FromSeconds(6)));

                if (completed == openTask)
                {
                    // Re-throw any exception from the open attempt to keep the same behavior.
                    await openTask;
                }
                else
                {
                    Console.WriteLine("[PROBE] WARNING: Connection attempt timed out; continuing.");
                }
            }
            catch (Exception)
            {
                // We only need the connector to initialize MiniCore; network success is not required.
            }

            if (!probe.MiniCoreLoaded)
            {
                Console.WriteLine("[PROBE] FAILURE: MiniCore load message not detected.");
                Environment.Exit(1);
            }

            Console.WriteLine("[PROBE] SUCCESS: Exiting verification app.");
            Environment.Exit(0);
        }
    }
}
