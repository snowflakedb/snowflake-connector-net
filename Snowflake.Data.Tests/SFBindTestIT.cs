/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
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
                conn.ConnectionString = ConnectionString;
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

        [Test]
        public void testBindNullValue()
        {
            using (SnowflakeDbConnection dbConnection = new SnowflakeDbConnection())
            {
                dbConnection.ConnectionString = ConnectionString;
                dbConnection.Open();
                try
                {
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        command.CommandText = "create or replace table TEST_TBL (ID number);";
                        command.ExecuteNonQuery();
                    }
                    
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        command.CommandText = "insert into TEST_TBL values(:p0)";
                        var param = command.CreateParameter();
                        param.ParameterName = "p0";
                        param.DbType = System.Data.DbType.Int32;
                        param.Value = DBNull.Value;
                        command.Parameters.Add(param);
                        int rowsInserted = command.ExecuteNonQuery();
                        Assert.AreEqual(1, rowsInserted);
                    }

                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        command.CommandText = "select ID from TEST_TBL;";
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            reader.Read();
                            Assert.IsTrue(reader.IsDBNull(0));
                            reader.Close();
                        }
                    }

                }
                finally
                {
                    using (IDbCommand command = dbConnection.CreateCommand())
                    {
                        command.CommandText = "drop table TEST_TBL";
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        [Test]
        public void testParameterCollection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p1.DbType = DbType.Int16;
                    p2.Value = 2;
                    

                    var p3 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p1.DbType = DbType.Int16;
                    p2.Value = 2;

                    Array parameters = Array.CreateInstance(typeof(IDbDataParameter), 3);
                    parameters.SetValue(p1, 0);
                    parameters.SetValue(p2, 1);
                    parameters.SetValue(p3, 2);

                    ((SnowflakeDbParameterCollection)cmd.Parameters).AddRange(parameters);
                    Assert.Throws<NotImplementedException>(
                        () => { cmd.Parameters.CopyTo(parameters, 5); });
    
                    Assert.AreEqual(3, cmd.Parameters.Count);
                    Assert.IsTrue(cmd.Parameters.Contains(p2));
                    Assert.IsTrue(cmd.Parameters.Contains("2"));
                    Assert.AreEqual(1, cmd.Parameters.IndexOf(p2));
                    Assert.AreEqual(1, cmd.Parameters.IndexOf("2"));

                    cmd.Parameters.Remove(p2);
                    Assert.AreEqual(2, cmd.Parameters.Count);
                    Assert.AreSame(p1, cmd.Parameters[0]);

                    cmd.Parameters.RemoveAt(0); 
                    Assert.AreSame(p3, cmd.Parameters[0]);

                    cmd.Parameters.Clear();
                    Assert.AreEqual(0, cmd.Parameters.Count);
                }

                conn.Close();
            }
        }
    }
}
