namespace Snowflake.Data.Tests.IntegrationTests
{
    using System.Data;
    using System.Data.Common;
    using System;
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System.Threading.Tasks;

    [TestFixture]
    class SFDbTransactionIT : SFBaseTest
    {
        [Test]
        public void TestTransactionDbConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Act
                using (IDbTransaction t1 = conn.BeginTransaction())
                {
                    // Assert
                    Assert.AreEqual(conn, t1.Connection);
                }
            }
        }

        [Test]
        public void TestTransactionIsolationLevel()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Act
                using (IDbTransaction t1 = conn.BeginTransaction())
                {
                    // Assert
                    Assert.AreEqual(IsolationLevel.ReadCommitted, t1.IsolationLevel);
                }
            }
        }

        [Test]
        // Test that when a transaction is disposed, rollback would be sent out
        public void TestTransactionDispose()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                CreateOrReplaceTable(conn, TableName, new[] { "c INT" });

                using (IDbTransaction t1 = conn.BeginTransaction())
                {
                    IDbCommand t1c1 = conn.CreateCommand();
                    t1c1.Transaction = t1;
                    t1c1.CommandText = $"insert into {TableName} values (1)";
                    t1c1.ExecuteNonQuery();
                }

                // Transaction t1 would be disposed and rollback at this point, tuple inserted is not visible
                IDbCommand c2 = conn.CreateCommand();
                c2.CommandText = $"SELECT * FROM {TableName}";
                IDataReader reader2 = c2.ExecuteReader();
                Assert.IsFalse(reader2.Read());
            }
        }

        [Test]
        // Test SNOW-761136 unnecessary ROLLBACK 
        public void TestTransactionRollback()
        {
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString;
            conn.Open();

            CreateOrReplaceTable(conn, TableName, new[]
            {
                "x TIMESTAMP_NTZ",
                "a INTEGER"
            });

            using (DbTransaction transaction = conn.BeginTransaction())
            {
                IDbCommand t1c1 = conn.CreateCommand();
                t1c1.Transaction = transaction;
                t1c1.CommandText = $"insert into {TableName} values (current_timestamp(), 1), (current_timestamp(), 2), (current_timestamp(), 3)";
                t1c1.ExecuteNonQuery();
                t1c1.Transaction.Commit();

                IDbCommand t1c2 = conn.CreateCommand();
                t1c2.Transaction = transaction;
                t1c2.CommandText = "BEGIN";
                t1c2.ExecuteNonQuery();

                IDbCommand t1c3 = conn.CreateCommand();
                t1c3.Transaction = transaction;
                t1c3.CommandText = $"insert into {TableName} values (current_timestamp(), 4)";
                t1c3.ExecuteNonQuery();
                t1c3.Transaction.Rollback();
            }

            IDbCommand command1 = conn.CreateCommand();
            command1.CommandText = $"Select * from {TableName}";
            IDataReader reader = command1.ExecuteReader();

            int row = 0;
            while (reader.Read())
            {
                var dataDate = reader.GetDateTime(0);
                var dataInt = reader.GetInt32(1);
                Console.Write("Row %d: %s, %d", row, dataDate.ToString(), dataInt);
                row++;
            }
            Assert.AreEqual(row, 4);

            conn.Close();
        }

        [Test]
        // Test SNOW-761136 unnecessary ROLLBACK 
        public void TestTransactionRollbackOn2Transactions()
        {
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString;
            conn.Open();

            CreateOrReplaceTable(conn, TableName, new[]
            {
                "x TIMESTAMP_NTZ",
                "a INTEGER"
            });

            using (DbTransaction transaction = conn.BeginTransaction())
            {
                IDbCommand t1c1 = conn.CreateCommand();
                t1c1.Transaction = transaction;
                t1c1.CommandText = $"insert into {TableName} values (current_timestamp(), 1), (current_timestamp(), 2), (current_timestamp(), 3)";
                t1c1.ExecuteNonQuery();
                t1c1.Transaction.Commit();
            }

            using (DbTransaction transaction2 = conn.BeginTransaction())
            {
                IDbCommand t2c2 = conn.CreateCommand();
                t2c2.Transaction = transaction2;
                t2c2.CommandText = $"insert into {TableName} values (current_timestamp(), 4)";
                t2c2.ExecuteNonQuery();
                t2c2.Transaction.Rollback();
            }

            IDbCommand command1 = conn.CreateCommand();
            command1.CommandText = $"Select * from {TableName}";
            IDataReader reader = command1.ExecuteReader();

            int row = 0;
            while (reader.Read())
            {
                var dataDate = reader.GetDateTime(0);
                var dataInt = reader.GetInt32(1);
                Console.Write("Row %d: %s, %d", row, dataDate.ToString(), dataInt);
                row++;
            }
            Assert.AreEqual(row, 3);

            conn.Close();
        }

        [Test]
        public void TestThrowsExceptionWhenBeginTransactionWithoutOpen()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                Assert.Throws<SnowflakeDbException>(() => conn.BeginTransaction());
            }
        }
    }
}
