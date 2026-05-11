namespace Snowflake.Data.Tests.IntegrationTests
{
    using Xunit;
    using Snowflake.Data.Client;
    using System.Data;
    using System.Runtime.InteropServices;
    class SFDbAdaptorIT : SFBaseTest
    {
        public SFDbAdaptorIT(TestEnvironmentFixture envFixture) : base(envFixture) { }

        private IDbDataAdapter _adapter;
        private SnowflakeDbCommand _command;
        public new void BeforeTest()
        {
            _adapter = new SnowflakeDbDataAdapter();
            _command = new SnowflakeDbCommand();
        }

        [Fact]
        public void TestCreatingDataAdapterWithSelectCommand()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            _adapter = new SnowflakeDbDataAdapter(_command);

            Assert.Equal(_command.CommandText, _adapter.SelectCommand.CommandText);
        }

        [Fact]
        public void TestCreatingDataAdapterWithSelectCommandTextAndConnection()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            SnowflakeDbConnection conn = new SnowflakeDbConnection(ConnectionString);
            _adapter = new SnowflakeDbDataAdapter(_command.CommandText, conn);

            Assert.Equal(_command.CommandText, _adapter.SelectCommand.CommandText);
            Assert.Equal(conn, _adapter.SelectCommand.Connection);
        }

        [Fact]
        public void TestSelectStatement()
        {
            DataSet ds = new DataSet("ds");
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                _adapter = new SnowflakeDbDataAdapter("select 1 as col1, 2 AS col2", conn);
                _adapter.Fill(ds);
                conn.Close();
            }
            Assert.Equal(ds.Tables[0].TableName, "Table");
            Assert.Equal(ds.Tables[0].Rows[0].ItemArray[0], 1);
            Assert.Equal(ds.Tables[0].Rows[0].ItemArray[1], 2);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Equal("1", ds.Tables[0].Rows[0]["col1"].ToString());
                Assert.Equal(ds.Tables[0].Rows[0]["col2"].ToString(), "2");
            }
        }

        [Fact]
        public void TestDataAdapterSetDeleteCommand()
        {
            _command.CommandText = "delete from table";
            _adapter.DeleteCommand = _command;

            Assert.Equal(_command, _adapter.DeleteCommand);
            Assert.Equal(_command.CommandText, _adapter.DeleteCommand.CommandText);
        }

        [Fact]
        public void TestDataAdapterSetInsertCommand()
        {
            _command.CommandText = "insert into table values (1, 2, 3)";
            _adapter.InsertCommand = _command;

            Assert.Equal(_command, _adapter.InsertCommand);
            Assert.Equal(_command.CommandText, _adapter.InsertCommand.CommandText);
        }

        [Fact]
        public void TestDataAdapterSetSelectCommand()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            _adapter.SelectCommand = _command;

            Assert.Equal(_command, _adapter.SelectCommand);
            Assert.Equal(_command.CommandText, _adapter.SelectCommand.CommandText);
        }

        [Fact]
        public void TestDataAdapterSetUpdateCommand()
        {
            _command.CommandText = "update table set col = 1 where col = 0";
            _adapter.UpdateCommand = _command;

            Assert.Equal(_command, _adapter.UpdateCommand);
            Assert.Equal(_command.CommandText, _adapter.UpdateCommand.CommandText);
        }
    }
}
