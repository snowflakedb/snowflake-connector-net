namespace Snowflake.Data.Tests.IntegrationTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using System.Runtime.InteropServices;

    [TestFixture]
    class SFDbAdaptorIT : SFBaseTest
    {
        private IDbDataAdapter _adapter;
        private SnowflakeDbCommand _command;

        [SetUp]
        public new void BeforeTest()
        {
            _adapter = new SnowflakeDbDataAdapter();
            _command = new SnowflakeDbCommand();
        }

        [Test]
        public void TestCreatingDataAdapterWithSelectCommand()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            _adapter = new SnowflakeDbDataAdapter(_command);

            Assert.AreEqual(_command.CommandText, _adapter.SelectCommand.CommandText);
        }

        [Test]
        public void TestCreatingDataAdapterWithSelectCommandTextAndConnection()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            SnowflakeDbConnection conn = new SnowflakeDbConnection(ConnectionString);
            _adapter = new SnowflakeDbDataAdapter(_command.CommandText, conn);

            Assert.AreEqual(_command.CommandText, _adapter.SelectCommand.CommandText);
            Assert.AreEqual(conn, _adapter.SelectCommand.Connection);
        }

        [Test]
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
            Assert.AreEqual(ds.Tables[0].TableName, "Table");
            Assert.AreEqual(ds.Tables[0].Rows[0].ItemArray[0], 1);
            Assert.AreEqual(ds.Tables[0].Rows[0].ItemArray[1], 2);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.AreEqual(ds.Tables[0].Rows[0]["col1"].ToString(), "1");
                Assert.AreEqual(ds.Tables[0].Rows[0]["col2"].ToString(), "2");
            }
        }

        [Test]
        public void TestDataAdapterSetDeleteCommand()
        {
            _command.CommandText = "delete from table";
            _adapter.DeleteCommand = _command;

            Assert.AreEqual(_command, _adapter.DeleteCommand);
            Assert.AreEqual(_command.CommandText, _adapter.DeleteCommand.CommandText);
        }

        [Test]
        public void TestDataAdapterSetInsertCommand()
        {
            _command.CommandText = "insert into table values (1, 2, 3)";
            _adapter.InsertCommand = _command;

            Assert.AreEqual(_command, _adapter.InsertCommand);
            Assert.AreEqual(_command.CommandText, _adapter.InsertCommand.CommandText);
        }

        [Test]
        public void TestDataAdapterSetSelectCommand()
        {
            _command.CommandText = "select 1 as col1, 2 AS col2";
            _adapter.SelectCommand = _command;

            Assert.AreEqual(_command, _adapter.SelectCommand);
            Assert.AreEqual(_command.CommandText, _adapter.SelectCommand.CommandText);
        }

        [Test]
        public void TestDataAdapterSetUpdateCommand()
        {
            _command.CommandText = "update table set col = 1 where col = 0";
            _adapter.UpdateCommand = _command;

            Assert.AreEqual(_command, _adapter.UpdateCommand);
            Assert.AreEqual(_command.CommandText, _adapter.UpdateCommand.CommandText);
        }
    }
}
