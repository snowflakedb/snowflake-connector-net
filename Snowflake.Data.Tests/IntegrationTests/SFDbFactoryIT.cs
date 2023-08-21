/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data;
    using System.Data.Common;

    [TestFixture]
    class SFDbFactoryIT : SFBaseTest
    {
        [Test]
        [Ignore("DbFactoryIT")]
        public void DbFactoryITDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestSimpleDbFactory()
        {
#if NETFRAMEWORK
            DbProviderFactory factory = DbProviderFactories.GetFactory("Snowflake.Data");
#else
            // In .NET Standard, DbProviderFactories is gone. 
            // Reference https://weblog.west-wind.com/posts/2017/Nov/27/Working-around-the-lack-of-dynamic-DbProviderFactory-loading-in-NET-Core
            // for more details
            DbProviderFactory factory = Snowflake.Data.Client.SnowflakeDbFactory.Instance;
#endif
            DbCommand command = factory.CreateCommand();
            DbConnection connection = factory.CreateConnection();
            connection.ConnectionString = ConnectionString;
            connection.Open();
            // set commnad's connection object
            command.Connection = connection;

            Assert.AreEqual(1, res);
        }

        [Test]
        public void TestDbFactoryWithParameter()
        {
            int expectedIntValue = 1;

            DbParameter parameter = _factory.CreateParameter();
            parameter.ParameterName = "1";
            parameter.Value = expectedIntValue;
            parameter.DbType = DbType.Int16;
            _command.Parameters.Add(parameter);

            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select ?";

            var result = _command.ExecuteScalar();

            Assert.AreEqual(expectedIntValue, result);
        }

        [Test]
        public void TestDbFactoryWithConnectionStringBuilder()
        {
            DbConnectionStringBuilder builder = _factory.CreateConnectionStringBuilder();
            builder.ConnectionString = ConnectionString;

            _connection.ConnectionString = builder.ConnectionString;
            _connection.Open();

            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            var result = _command.ExecuteScalar();

            Assert.AreEqual(1, result);
        }

        [Test]
        public void TestDbFactoryWithCommandBuilderAndAdapter()
        {
            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            DbDataAdapter adapter = _factory.CreateDataAdapter();
            adapter.SelectCommand = _command;
            DbCommandBuilder builder = _factory.CreateCommandBuilder();
            builder.DataAdapter = adapter;
            DataSet ds = new DataSet("ds");

            adapter.Fill(ds);

            Assert.AreEqual(1, ds.Tables[0].Rows[0].ItemArray[0]);
        }
    }
}
