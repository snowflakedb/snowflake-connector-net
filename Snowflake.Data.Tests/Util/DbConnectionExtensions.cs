using System.Data;
using System.Data.Common;
using Snowflake.Data.Client;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.IcebergTests;

namespace Snowflake.Data.Tests.Util
{
    public static class DbConnectionExtensions
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<TestIcebergTable>();

        internal static IDbCommand CreateCommand(this IDbConnection connection, string commandText)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandText = commandText;
            s_logger.Debug(commandText);
            return command;
        }

        internal static int ExecuteNonQuery(this IDbConnection connection, string commandText)
        {
            var rowsAffected = connection.CreateCommand(commandText).ExecuteNonQuery();
            s_logger.Debug($"Affected row(s): {rowsAffected}");
            return rowsAffected;
        }

        public static DbDataReader ExecuteReader(this SnowflakeDbConnection connection, string commandText)
            => (DbDataReader)connection.CreateCommand(commandText).ExecuteReader();
    }
}
