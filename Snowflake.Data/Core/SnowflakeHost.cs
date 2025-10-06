using System;

namespace Snowflake.Data.Core
{
    internal static class SnowflakeHost
    {
        public const string DefaultHost = "snowflakecomputing.com";
        private const string AlternativeHost = "snowflakecomputing.cn";

        public static bool IsSnowflakeHost(string host) =>
            host.EndsWith($".{DefaultHost}", StringComparison.OrdinalIgnoreCase) ||
            host.EndsWith($".{AlternativeHost}", StringComparison.OrdinalIgnoreCase);
    }
}
