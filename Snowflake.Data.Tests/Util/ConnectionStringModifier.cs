using System.Text.RegularExpressions;

namespace Snowflake.Data.Tests.Util
{
    public static class ConnectionStringModifier
    {
        public static string DisableCrlRevocationCheck(string connectionString)
        {
            var modifiedConnectionString = Regex.Replace(connectionString, "certRevocationCheckMode=enabled", "certRevocationCheckMode=disabled", RegexOptions.IgnoreCase);
            if (connectionString != modifiedConnectionString)
                return modifiedConnectionString;
            return connectionString + "certRevocationCheckMode=disabled;";
        }
    }
}
