using System.Data;

namespace Snowflake.Data.Tests.Util
{
    public static class DbConnectionTestExtensions
    {
        internal static IDbCommand CreateCommand(this IDbConnection connection, string commandText)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandText = commandText;
            return command;
        }
        
        internal static int ExecuteNonQuery(this IDbConnection connection, string commandText)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandText = commandText;
            return command.ExecuteNonQuery();
        }
    }
}
