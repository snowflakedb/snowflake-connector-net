using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests;

#if NETFRAMEWORK
public static class FrameworkShims
{
    public static Task<DbTransaction> BeginTransactionAsync(this SnowflakeDbConnection connection) => Task.FromResult(connection.BeginTransaction());

    public static Task CloseAsync(this DbConnection connection)
    {
        connection.Close();
        return Task.CompletedTask;
    }

    public static Task CloseAsync(this DbDataReader reader)
    {
        reader.Close();
        return Task.CompletedTask;
    }

    public static Task<DbTransaction> BeginTransactionAsync(this DbConnection connection, IsolationLevel isolationLevel)
    {
        var result = connection.BeginTransaction(isolationLevel);
        return Task.FromResult(result);
    }
}
#endif
