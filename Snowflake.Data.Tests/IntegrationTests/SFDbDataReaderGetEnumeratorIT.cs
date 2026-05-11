using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFDbDataReaderGetEnumeratorITJson : SFDbDataReaderGetEnumeratorIT
    {
        public SFDbDataReaderGetEnumeratorITJson(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture) : base(fixture, envFixture, ResultFormat.JSON) { }
    }

    public sealed class SFDbDataReaderGetEnumeratorITArrow : SFDbDataReaderGetEnumeratorIT
    {
        public SFDbDataReaderGetEnumeratorITArrow(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture) : base(fixture, envFixture, ResultFormat.ARROW) { }
    }

    public abstract class SFDbDataReaderGetEnumeratorIT : SFBaseTest
    {

        private readonly ResultFormat _resultFormat;

        private readonly SFBaseTestAsyncFixture _fixture;
        public SFDbDataReaderGetEnumeratorIT(SFBaseTestAsyncFixture fixture, TestEnvironmentFixture envFixture, ResultFormat resultFormat) : base(fixture, envFixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
        }

        [Fact]
        public void TestGetEnumerator()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {_fixture.TableName}";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.True(enumerator.MoveNext());
                Assert.Equal(3, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.True(enumerator.MoveNext());
                Assert.Equal(5, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.True(enumerator.MoveNext());
                Assert.Equal(8, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.False(enumerator.MoveNext());

                reader.Close();

                DropTestTableAndCloseConnection(conn);
            }
        }

        [Fact]
        public void TestGetEnumeratorShouldBeEmptyWhenNotRowsReturned()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {_fixture.TableName} WHERE cola > 10";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.False(enumerator.MoveNext());
                Assert.Null(enumerator.Current);

                reader.Close();
                DropTestTableAndCloseConnection(conn);
            }
        }

        [Fact]
        public void TestGetEnumeratorWithCastMethod()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {_fixture.TableName}";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var dataRecords = reader.Cast<DbDataRecord>().ToList();
                Assert.Equal(3, dataRecords.Count);

                reader.Close();

                DropTestTableAndCloseConnection(conn);
            }
        }

        [Fact]
        public void TestGetEnumeratorForEachShouldNotEnterWhenResultsIsEmpty()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {_fixture.TableName} WHERE cola > 10";
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

        [Fact]
        public void TestGetEnumeratorShouldThrowNonSupportedExceptionWhenReset()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateAndPopulateTestTable(conn);

                string selectCommandText = $"select * from {_fixture.TableName}";
                IDbCommand selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = selectCmd.ExecuteReader() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.True(enumerator.MoveNext());

                Assert.Throws<NotSupportedException>(() => enumerator.Reset());

                reader.Close();

                DropTestTableAndCloseConnection(conn);
            }
        }

        private void DropTestTableAndCloseConnection(DbConnection conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = $"drop table if exists {_fixture.TableName}";
            var count = cmd.ExecuteNonQuery();
            Assert.Equal(0, count);

            CloseConnection(conn);
        }

        private void CreateAndPopulateTestTable(DbConnection conn)
        {
            _fixture.CreateOrReplaceTable(conn, _fixture.TableName, new[] { "cola NUMBER" });

            var cmd = conn.CreateCommand();

            string insertCommand = $"insert into {_fixture.TableName} values (3),(5),(8)";
            cmd.CommandText = insertCommand;
            cmd.ExecuteNonQuery();
        }

        private DbConnection CreateAndOpenConnection()
        {
            var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
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
