using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using Xunit;
    using Snowflake.Data.Client;
    using System.Data;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    public sealed class SFDbAdaptorIT : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private IDbDataAdapter _adapter;
        private SnowflakeDbCommand _command;

        public SFDbAdaptorIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            _adapter = new SnowflakeDbDataAdapter();
            _command = new SnowflakeDbCommand();
        }

        [SFFact]
        public async Task TestCreatingDataAdapterWithSelectCommand()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            _adapter = new SnowflakeDbDataAdapter(_command);

            Assert.Equal(_command.CommandText, _adapter.SelectCommand.CommandText);
        }

        [SFFact]
        public async Task TestCreatingDataAdapterWithSelectCommandTextAndConnection()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            SnowflakeDbConnection conn = new SnowflakeDbConnection(_fixture.ConnectionString);
            _adapter = new SnowflakeDbDataAdapter(_command.CommandText, conn);

            Assert.Equal(_command.CommandText, _adapter.SelectCommand.CommandText);
            Assert.Equal(conn, _adapter.SelectCommand.Connection);
        }

        [SFFact]
        public async Task TestSelectStatement()
        {
            DataSet ds = new DataSet("ds");
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

                _adapter = new SnowflakeDbDataAdapter("select 1 as col1, 2 AS col2", conn);
                _adapter.Fill(ds);
                await conn.CloseAsync(CancellationToken.None);
            }
            Assert.Equal("Table", ds.Tables[0].TableName);
            Assert.Equal(ds.Tables[0].Rows[0].ItemArray[0], 1L);
            Assert.Equal(ds.Tables[0].Rows[0].ItemArray[1], 2L);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Equal("1", ds.Tables[0].Rows[0]["col1"].ToString());
                Assert.Equal("2", ds.Tables[0].Rows[0]["col2"].ToString());
            }
        }

        [SFFact]
        public async Task TestDataAdapterSetDeleteCommand()
        {
            _command.CommandText = "delete from table";
            _adapter.DeleteCommand = _command;

            Assert.Equal(_command, _adapter.DeleteCommand);
            Assert.Equal(_command.CommandText, _adapter.DeleteCommand.CommandText);
        }

        [SFFact]
        public async Task TestDataAdapterSetInsertCommand()
        {
            _command.CommandText = "insert into table values (1, 2, 3)";
            _adapter.InsertCommand = _command;

            Assert.Equal(_command, _adapter.InsertCommand);
            Assert.Equal(_command.CommandText, _adapter.InsertCommand.CommandText);
        }

        [SFFact]
        public async Task TestDataAdapterSetSelectCommand()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            _adapter.SelectCommand = _command;

            Assert.Equal(_command, _adapter.SelectCommand);
            Assert.Equal(_command.CommandText, _adapter.SelectCommand.CommandText);
        }

        [SFFact]
        public async Task TestDataAdapterSetUpdateCommand()
        {
            _command.CommandText = "update table set col = 1 where col = 0";
            _adapter.UpdateCommand = _command;

            Assert.Equal(_command, _adapter.UpdateCommand);
            Assert.Equal(_command.CommandText, _adapter.UpdateCommand.CommandText);
        }
    }
}
