using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture(ResultFormat.ARROW)]
    [TestFixture(ResultFormat.JSON)]
    class SFDbDataReaderGetEnumeratorIT : SFBaseTest
    {
        protected override string TestName => base.TestName + _resultFormat;

        private readonly ResultFormat _resultFormat;

        public SFDbDataReaderGetEnumeratorIT(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }

        [Test]
        public void TestGetEnumerator()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {TableName}";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(3, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(5, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(8, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.IsFalse(enumerator.MoveNext());

                reader.Close();

                DropTestTableAndCloseConnection(conn);
            }
        }

        [Test]
        public void TestGetEnumeratorShouldBeEmptyWhenNotRowsReturned()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {TableName} WHERE cola > 10";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.IsFalse(enumerator.MoveNext());
                Assert.IsNull(enumerator.Current);

                reader.Close();
                DropTestTableAndCloseConnection(conn);
            }
        }

        [Test]
        public void TestGetEnumeratorWithCastMethod()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {TableName}";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var dataRecords = reader.Cast<DbDataRecord>().ToList();
                Assert.AreEqual(3, dataRecords.Count);

                reader.Close();

                DropTestTableAndCloseConnection(conn);
            }
        }

        [Test]
        public void TestGetEnumeratorForEachShouldNotEnterWhenResultsIsEmpty()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {TableName} WHERE cola > 10";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                foreach (var record in reader)
                {
                    Assert.Fail("Should not enter when results is empty");
                }

                reader.Close();
                DropTestTableAndCloseConnection(conn);
            }
        }

        [Test]
        public void TestGetEnumeratorShouldThrowNonSupportedExceptionWhenReset()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {TableName}";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.IsTrue(enumerator.MoveNext());

                Assert.Throws<NotSupportedException>(() => enumerator.Reset());

                reader.Close();

                DropTestTableAndCloseConnection(conn);
            }
        }

        private void DropTestTableAndCloseConnection(DbConnection conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = $"drop table if exists {TableName}";
            var count = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, count);

            CloseConnection(conn);
        }

        private void CreateAndPopulateTestTable(DbConnection conn)
        {
            CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER" });

            var cmd = conn.CreateCommand();

            string insertCommand = $"insert into {TableName} values (3),(5),(8)";
            cmd.CommandText = insertCommand;
            cmd.ExecuteNonQuery();
        }

        private DbConnection CreateAndOpenConnection()
        {
            var conn = new SnowflakeDbConnection(ConnectionString);
            conn.Open();
            SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
            return conn;
        }

        private void CloseConnection(DbConnection conn)
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
            conn.Close();
        }
    }
}
