using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
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

    public static Task ChangeDatabaseAsync(this DbConnection connection, string databaseName)
    {
        connection.ChangeDatabase(databaseName);
        return Task.CompletedTask;
    }

    public static Task ChangeDatabaseAsync(this DbConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        connection.ChangeDatabase(databaseName);
        return Task.CompletedTask;
    }

    public static Task CommitAsync(this DbTransaction transaction)
    {
        transaction.Commit();
        return Task.CompletedTask;
    }

    public static Task RollbackAsync(this DbTransaction transaction)
    {
        transaction.Rollback();
        return Task.CompletedTask;
    }

    public static bool TryDequeue<T>(this Queue<T> queue, out T element)
    {
        element = default(T);
        if (queue.Count == 0)
            return false;

        element = queue.Dequeue();
        return true;
    }
}
#endif
