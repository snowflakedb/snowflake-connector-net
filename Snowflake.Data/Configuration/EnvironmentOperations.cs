using System;

namespace Snowflake.Data.Configuration
{
    internal class EnvironmentOperations
    {
        public virtual string GetEnvironmentVariable(string variable)
        {
            return Environment.GetEnvironmentVariable(variable);
        }

        public virtual string ExpandEnvironmentVariables(string name)
        {
            return Environment.ExpandEnvironmentVariables(name);
        }
    }
}
