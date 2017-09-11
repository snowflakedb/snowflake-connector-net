/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Data;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;

    [TestFixture]    
    class SFBindTestIT : SFBaseTest
    {
        [Test]
        public void testArrayBind()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "create or replace table testArrayBind(cola integer, colb string)";
                    int count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(0, count);

                    string insertCommand = "insert into testArrayBind values (?, ?)";
                    cmd.CommandText = insertCommand;

                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = new int[] { 1, 2, 3 };
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = new string[] { "str1", "str2", "str3" };
                    cmd.Parameters.Add(p2);

                    count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(3, count);

                    cmd.CommandText = "drop table if exists testArrayBind";
                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            }
        }
    }
}
