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

            command.CommandText = "select 1";
            object res = command.ExecuteScalar();
            Assert.AreEqual(1, res);

            connection.Close();
        }
    }
}
