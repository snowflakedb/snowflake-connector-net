using System.Data;

namespace Snowflake.Data.Tests.Util
{
    public static class DbCommandTestExtensions
    {
        internal static IDbDataParameter Add(this IDbCommand command, string name, DbType dbType, object value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = dbType;
            parameter.Value = value;
            command.Parameters.Add(parameter);
            return parameter;
        }

    }
}
