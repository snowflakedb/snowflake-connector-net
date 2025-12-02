using System;
using Snowflake.Data.Client;
using Microsoft.Extensions.Logging;

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
        static void Main(string[] args)
        {
            var probe = new ProbeLogger();
            SnowflakeDbLoggerConfig.SetCustomLogger(probe);

            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = "account=dummy;user=dummy;password=dummy;host=localhost;scheme=http";
                    conn.Open(); 
                }
            }
            catch (Exception) { }

            if (!probe.MiniCoreLoaded)
            {
                Console.WriteLine("[PROBE] FAILURE: MiniCore load message not detected.");
                Environment.Exit(1);
            }
        }
    }
}
