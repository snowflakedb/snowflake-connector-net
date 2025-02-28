using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal class TargetFrameworkReporter
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<TargetFrameworkReporter>();

        public static void Report()
        {
#if NETSTANDARD2_1
            s_logger.Warn($"Using library targeted to netstandard 2.1");
#endif
#if NETSTANDARD2_0
            s_logger.Warn($"Using library targeted to netstandard 2.0");
#endif
        }
    }
}
