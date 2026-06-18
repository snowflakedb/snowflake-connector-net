using System.Threading;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using System.Data;
    using System.Data.Common;
    using System;
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System.Threading.Tasks;
    public class SFDbTransactionIT : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFDbTransactionIT(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        [SFFact]
        public async Task TestTransactionDbConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

                // Act
                using (IDbTransaction t1 = conn.BeginTransaction())
                {
                    // Assert
                    Assert.Equal(conn, t1.Connection);
                }
            }
        }

        [SFFact]
        public async Task TestTransactionIsolationLevel()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

                // Act
                using (IDbTransaction t1 = conn.BeginTransaction())
                {
                    // Assert
                    Assert.Equal(IsolationLevel.ReadCommitted, t1.IsolationLevel);
                }
            }
        }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        // Test that when a transaction is disposed, rollback would be sent out
        public async Task TestTransactionDispose()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

                await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "c INT" });

                using (IDbTransaction t1 = await conn.BeginTransactionAsync())
                {
                    IDbCommand t1c1 = conn.CreateCommand();
                    t1c1.Transaction = t1;
                    t1c1.CommandText = $"insert into {tableName} values (1)";
                    t1c1.ExecuteNonQuery();
                }

                // Transaction t1 would be disposed and rollback at this point, tuple inserted is not visible
                IDbCommand c2 = conn.CreateCommand();
                c2.CommandText = $"SELECT * FROM {tableName}";
                IDataReader reader2 = c2.ExecuteReader();
                Assert.False(reader2.Read());
            }
        }

        [SFFact]
        // Test SNOW-761136 unnecessary ROLLBACK
        public async Task TestTransactionRollback()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString;
            await conn.OpenAsync(CancellationToken.None);

            await _fixture.CreateOrReplaceTable(conn, tableName, new[]
            {
                "x TIMESTAMP_NTZ",
                "a INTEGER"
            });

            using (DbTransaction transaction = await conn.BeginTransactionAsync())
            {
                IDbCommand t1c1 = conn.CreateCommand();
                t1c1.Transaction = transaction;
                t1c1.CommandText = $"insert into {tableName} values (current_timestamp(), 1), (current_timestamp(), 2), (current_timestamp(), 3)";
                t1c1.ExecuteNonQuery();
                t1c1.Transaction.Commit();

                IDbCommand t1c2 = conn.CreateCommand();
                t1c2.Transaction = transaction;
                t1c2.CommandText = "BEGIN";
                t1c2.ExecuteNonQuery();

                IDbCommand t1c3 = conn.CreateCommand();
                t1c3.Transaction = transaction;
                t1c3.CommandText = $"insert into {tableName} values (current_timestamp(), 4)";
                t1c3.ExecuteNonQuery();
                t1c3.Transaction.Rollback();
            }

            IDbCommand command1 = conn.CreateCommand();
            command1.CommandText = $"Select * from {tableName}";
            IDataReader reader = command1.ExecuteReader();

            int row = 0;
            while (reader.Read())
            {
                var dataDate = reader.GetDateTime(0);
                var dataInt = reader.GetInt32(1);
                Console.Write("Row %d: %s, %d", row, dataDate.ToString(), dataInt);
                row++;
            }
            Assert.Equal(4, row);

            await conn.CloseAsync(CancellationToken.None);
        }

        [SFFact(RetriesCount = RetriesCount.Once)]
        // Test SNOW-761136 unnecessary ROLLBACK
        public async Task TestTransactionRollbackOn2Transactions()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString;
            await conn.OpenAsync(CancellationToken.None);

            await _fixture.CreateOrReplaceTable(conn, tableName, new[]
            {
                "x TIMESTAMP_NTZ",
                "a INTEGER"
            });

            using (DbTransaction transaction = conn.BeginTransaction())
            {
                IDbCommand t1c1 = conn.CreateCommand();
                t1c1.Transaction = transaction;
                t1c1.CommandText = $"insert into {tableName} values (current_timestamp(), 1), (current_timestamp(), 2), (current_timestamp(), 3)";
                t1c1.ExecuteNonQuery();
                t1c1.Transaction.Commit();
            }

            using (DbTransaction transaction2 = await conn.BeginTransactionAsync())
            {
                IDbCommand t2c2 = conn.CreateCommand();
                t2c2.Transaction = transaction2;
                t2c2.CommandText = $"insert into {tableName} values (current_timestamp(), 4)";
                t2c2.ExecuteNonQuery();
                t2c2.Transaction.Rollback();
            }

            IDbCommand command1 = conn.CreateCommand();
            command1.CommandText = $"Select * from {tableName}";
            IDataReader reader = command1.ExecuteReader();

            int row = 0;
            while (reader.Read())
            {
                var dataDate = reader.GetDateTime(0);
                var dataInt = reader.GetInt32(1);
                Console.Write("Row %d: %s, %d", row, dataDate.ToString(), dataInt);
                row++;
            }
            Assert.Equal(3, row);

            await conn.CloseAsync(CancellationToken.None);
        }

        [SFFact]
        public async Task TestThrowsExceptionWhenBeginTransactionWithoutOpen()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                Assert.Throws<SnowflakeDbException>(() => conn.BeginTransaction());
            }
        }
    }
}
