using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFDbDataReaderGetEnumeratorITJson : SFDbDataReaderGetEnumeratorIT
    {
        public SFDbDataReaderGetEnumeratorITJson(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.JSON) { }
    }

    public sealed class SFDbDataReaderGetEnumeratorITArrow : SFDbDataReaderGetEnumeratorIT
    {
        public SFDbDataReaderGetEnumeratorITArrow(SFBaseTestAsyncFixture fixture) : base(fixture, ResultFormat.ARROW) { }
    }

    public abstract class SFDbDataReaderGetEnumeratorIT : SFBaseTestAsync
    {

        private readonly ResultFormat _resultFormat;

        private readonly SFBaseTestAsyncFixture _fixture;
        public SFDbDataReaderGetEnumeratorIT(SFBaseTestAsyncFixture fixture, ResultFormat resultFormat) : base(fixture)
        {
            _fixture = fixture;
            _resultFormat = resultFormat;
        }

        [Fact]
        public async Task TestGetEnumerator()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await CreateAndPopulateTestTableAsync(conn);

                string selectCommandText = $"select * from {_fixture.TableName}";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.True(enumerator.MoveNext());
                Assert.Equal(3, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.True(enumerator.MoveNext());
                Assert.Equal(5, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.True(enumerator.MoveNext());
                Assert.Equal(8, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.False(enumerator.MoveNext());

                reader.Close();

                await DropTestTableAndCloseConnectionAsync(conn);
            }
        }

        [Fact]
        public async Task TestGetEnumeratorShouldBeEmptyWhenNotRowsReturned()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await CreateAndPopulateTestTableAsync(conn);

                string selectCommandText = $"select * from {_fixture.TableName} WHERE cola > 10";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.False(enumerator.MoveNext());
                Assert.Null(enumerator.Current);

                reader.Close();
                await DropTestTableAndCloseConnectionAsync(conn);
            }
        }

        [Fact]
        public async Task TestGetEnumeratorWithCastMethod()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await CreateAndPopulateTestTableAsync(conn);

                string selectCommandText = $"select * from {_fixture.TableName}";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync() as DbDataReader;

                var dataRecords = reader.Cast<DbDataRecord>().ToList();
                Assert.Equal(3, dataRecords.Count);

                reader.Close();

                await DropTestTableAndCloseConnectionAsync(conn);
            }
        }

        [Fact]
        public async Task TestGetEnumeratorForEachShouldNotEnterWhenResultsIsEmpty()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await CreateAndPopulateTestTableAsync(conn);

                string selectCommandText = $"select * from {_fixture.TableName} WHERE cola > 10";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync() as DbDataReader;

                foreach (var record in reader)
                {
                    Assert.Fail("Should not enter when results is empty");
                }

                reader.Close();
                await DropTestTableAndCloseConnectionAsync(conn);
            }
        }

        [Fact]
        public async Task TestGetEnumeratorShouldThrowNonSupportedExceptionWhenReset()
        {
            using (var conn = await CreateAndOpenConnectionAsync())
            {
                await CreateAndPopulateTestTableAsync(conn);

                string selectCommandText = $"select * from {_fixture.TableName}";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync() as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.True(enumerator.MoveNext());

                Assert.Throws<NotSupportedException>(() => enumerator.Reset());

                reader.Close();

                await DropTestTableAndCloseConnectionAsync(conn);
            }
        }

        private async Task DropTestTableAndCloseConnectionAsync(DbConnection conn)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"drop table if exists {_fixture.TableName}";
            var count = await cmd.ExecuteNonQueryAsync();
            Assert.Equal(0, count);

            await CloseConnectionAsync(conn);
        }

        private async Task CreateAndPopulateTestTableAsync(DbConnection conn)
        {
            _fixture.CreateOrReplaceTable(conn, _fixture.TableName, new[] { "cola NUMBER" });

            var cmd = conn.CreateCommand();

            string insertCommand = $"insert into {_fixture.TableName} values (3),(5),(8)";
            cmd.CommandText = insertCommand;
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task<DbConnection> CreateAndOpenConnectionAsync()
        {
            var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn.OpenAsync(CancellationToken.None);
            SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
            return conn;
        }

        private async Task CloseConnectionAsync(DbConnection conn)
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
            await conn.CloseAsync();
        }
    }
}
