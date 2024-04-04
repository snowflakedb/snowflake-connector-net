using System.Data;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.Util
{
    public static class DbCommandExtensions
    {
        internal static SnowflakeDbParameter Add(this IDbCommand command, string name, DbType dbType, object value)
        {
            var parameter = (SnowflakeDbParameter)command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            parameter.Value = value;
            command.Parameters.Add(parameter);
            return parameter;
        }

    }
}
