using System;

namespace Snowflake.Data.Tests.Util
{
    public static class ConnectionStringModifier
    {
        public static string DisableCrlRevocationCheck(string connectionString)
        {
            var modifiedConnectionString = connectionString.Replace("certRevocationCheckMode=enabled", "certRevocationCheckMode=disabled", StringComparison.InvariantCultureIgnoreCase);
            if (connectionString != modifiedConnectionString)
                return modifiedConnectionString;
            return connectionString + "certRevocationCheckMode=disabled;";
        }
    }
}
