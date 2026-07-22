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

        [SFFact]
        public async Task TestGetEnumerator()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = await CreateAndOpenConnectionAsync().ConfigureAwait(false))
            {
                await CreateAndPopulateTestTableAsync(conn, tableName).ConfigureAwait(false);

                string selectCommandText = $"select * from {tableName}";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync().ConfigureAwait(false) as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.True(enumerator.MoveNext());
                Assert.Equal(3, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.True(enumerator.MoveNext());
                Assert.Equal(5, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.True(enumerator.MoveNext());
                Assert.Equal(8, (enumerator.Current as DbDataRecord).GetInt64(0));
                Assert.False(enumerator.MoveNext());

                reader.Close();

                await DropTestTableAndCloseConnectionAsync(conn, tableName).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetEnumeratorShouldBeEmptyWhenNotRowsReturned()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = await CreateAndOpenConnectionAsync().ConfigureAwait(false))
            {
                await CreateAndPopulateTestTableAsync(conn, tableName).ConfigureAwait(false);

                string selectCommandText = $"select * from {tableName} WHERE cola > 10";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync().ConfigureAwait(false) as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.False(enumerator.MoveNext());
                Assert.Null(enumerator.Current);

                reader.Close();
                await DropTestTableAndCloseConnectionAsync(conn, tableName).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetEnumeratorWithCastMethod()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = await CreateAndOpenConnectionAsync().ConfigureAwait(false))
            {
                await CreateAndPopulateTestTableAsync(conn, tableName).ConfigureAwait(false);

                string selectCommandText = $"select * from {tableName}";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync().ConfigureAwait(false) as DbDataReader;

                var dataRecords = reader.Cast<DbDataRecord>().ToList();
                Assert.Equal(3, dataRecords.Count);

                reader.Close();

                await DropTestTableAndCloseConnectionAsync(conn, tableName).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetEnumeratorForEachShouldNotEnterWhenResultsIsEmpty()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = await CreateAndOpenConnectionAsync().ConfigureAwait(false))
            {
                await CreateAndPopulateTestTableAsync(conn, tableName).ConfigureAwait(false);

                string selectCommandText = $"select * from {tableName} WHERE cola > 10";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync().ConfigureAwait(false);

                foreach (var record in reader)
                {
                    Assert.Fail("Should not enter when results is empty");
                }

                await reader.CloseAsync().ConfigureAwait(false);
                await DropTestTableAndCloseConnectionAsync(conn, tableName).ConfigureAwait(false);
            }
        }

        [SFFact]
        public async Task TestGetEnumeratorShouldThrowNonSupportedExceptionWhenReset()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = await CreateAndOpenConnectionAsync().ConfigureAwait(false))
            {
                await CreateAndPopulateTestTableAsync(conn, tableName).ConfigureAwait(false);

                string selectCommandText = $"select * from {tableName}";
                var selectCmd = conn.CreateCommand();
                selectCmd.CommandText = selectCommandText;
                DbDataReader reader = await selectCmd.ExecuteReaderAsync().ConfigureAwait(false) as DbDataReader;

                var enumerator = reader.GetEnumerator();
                Assert.True(enumerator.MoveNext());

                Assert.Throws<NotSupportedException>(() => enumerator.Reset());

                reader.Close();

                await DropTestTableAndCloseConnectionAsync(conn, tableName).ConfigureAwait(false);
            }
        }

        private async Task DropTestTableAndCloseConnectionAsync(DbConnection conn, string tableName)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"drop table if exists {tableName}";
            var count = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            Assert.Equal(0, count);

            await CloseConnectionAsync(conn).ConfigureAwait(false);
        }

        private async Task CreateAndPopulateTestTableAsync(SnowflakeDbConnection conn, string tableName)
        {
            await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "cola NUMBER" }).ConfigureAwait(false);

            var cmd = conn.CreateCommand();

            string insertCommand = $"insert into {tableName} values (3),(5),(8)";
            cmd.CommandText = insertCommand;
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        private async Task<SnowflakeDbConnection> CreateAndOpenConnectionAsync()
        {
            var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
            return conn;
        }

        private async Task CloseConnectionAsync(DbConnection conn)
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }
}
