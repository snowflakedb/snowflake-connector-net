/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using NUnit.Framework;

    [TestFixture]
    class SFMultiStatementsIT : SFBaseTest
    {
        [Test]
        [Ignore("MultiStatementsIT")]
        public void MultiStatementsITDone()
        {
            // Do nothing;
        }

        [Test]
        public void testSelectWithoutBinding()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                var param = cmd.CreateParameter();
                param.ParameterName = "MULTI_STATEMENT_COUNT";
                param.DbType = DbType.Int16;
                param.Value = 3;
                cmd.Parameters.Add(param);
                cmd.CommandText = "select 1; select 2, 3; select 4, 5, 6";
                IDataReader reader = cmd.ExecuteReader();

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(1, reader.GetInt32(0));
                Assert.IsFalse(reader.Read());

                Assert.IsTrue(reader.NextResult());
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(2, reader.GetInt32(0));
                Assert.AreEqual(3, reader.GetInt32(1));
                Assert.IsFalse(reader.Read());

                Assert.IsTrue(reader.NextResult());
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(4, reader.GetInt32(0));
                Assert.AreEqual(5, reader.GetInt32(1));
                Assert.AreEqual(6, reader.GetInt32(2));
                Assert.IsFalse(reader.Read());

                Assert.IsFalse(reader.NextResult());

                reader.Close();
                conn.Close();
            }
        }

        [Test]
        public void testSelectWithBinding()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                // Set statement count
                var stmtCountParam = cmd.CreateParameter();
                stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                stmtCountParam.DbType = DbType.Int16;
                stmtCountParam.Value = 3;
                cmd.Parameters.Add(stmtCountParam);

                // set parameter bindings
                for (int i = 1; i <= 6; i++)
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = i.ToString();
                    param.DbType = DbType.Int16;
                    param.Value = i;
                    cmd.Parameters.Add(param);
                }
                cmd.CommandText = "select ?; select ?, ?; select ?, ?, ?";
                IDataReader reader = cmd.ExecuteReader();

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(1, reader.GetInt32(0));
                Assert.IsFalse(reader.Read());

                Assert.IsTrue(reader.NextResult());
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(2, reader.GetInt32(0));
                Assert.AreEqual(3, reader.GetInt32(1));
                Assert.IsFalse(reader.Read());

                Assert.IsTrue(reader.NextResult());
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(4, reader.GetInt32(0));
                Assert.AreEqual(5, reader.GetInt32(1));
                Assert.AreEqual(6, reader.GetInt32(2));
                Assert.IsFalse(reader.Read());

                Assert.IsFalse(reader.NextResult());

                reader.Close();
                conn.Close();
            }
        }

        [Test]
        public void testMixedQueryTypeWithBinding()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "create or replace table testMSArrayBind(cola integer, colb string);" +
                                      "insert into testMSArrayBind values (?, ?);" +
                                      "insert into testMSArrayBind values (?, ?), (?, ?);" +
                                      "select * from testMSArrayBind;" +
                                      "drop table if exists testMSArrayBind";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 5;
                    cmd.Parameters.Add(stmtCountParam);

                    // set parameter bindings
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value ="str1";
                    cmd.Parameters.Add(p2);

                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int16;
                    p3.Value = 2;
                    cmd.Parameters.Add(p3);

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.String;
                    p4.Value = "str2";
                    cmd.Parameters.Add(p4);

                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.Int16;
                    p5.Value = 3;
                    cmd.Parameters.Add(p5);

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.String;
                    p6.Value = "str3";
                    cmd.Parameters.Add(p6);

                    DbDataReader reader = cmd.ExecuteReader();

                    // result of create
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of insert #1
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(1, reader.RecordsAffected);

                    // result of insert #2
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(2, reader.RecordsAffected);

                    // result of select
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(-1, reader.RecordsAffected);
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual("str1", reader.GetString(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(2, reader.GetInt32(0));
                    Assert.AreEqual("str2", reader.GetString(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(3, reader.GetInt32(0));
                    Assert.AreEqual("str3", reader.GetString(1));
                    Assert.IsFalse(reader.Read());

                    // result of drop
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    Assert.IsFalse(reader.NextResult());
                    reader.Close();
                }

                conn.Close();
            }
        }

        [Test]
        public void testWithExecuteNonQuery()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "create or replace temporary table testMSArrayBind(cola integer, colb string);" +
                                      "insert into testMSArrayBind values (?, ?);" +
                                      "insert into testMSArrayBind values (?, ?), (?, ?);" +
                                      "select * from testMSArrayBind;" +
                                      "drop table if exists testMSArrayBind";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 5;
                    cmd.Parameters.Add(stmtCountParam);

                    // set parameter bindings
                    var p1 = cmd.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.Int16;
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    var p2 = cmd.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value ="str1";
                    cmd.Parameters.Add(p2);

                    var p3 = cmd.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.Int16;
                    p3.Value = 2;
                    cmd.Parameters.Add(p3);

                    var p4 = cmd.CreateParameter();
                    p4.ParameterName = "4";
                    p4.DbType = DbType.String;
                    p4.Value = "str2";
                    cmd.Parameters.Add(p4);

                    var p5 = cmd.CreateParameter();
                    p5.ParameterName = "5";
                    p5.DbType = DbType.Int16;
                    p5.Value = 3;
                    cmd.Parameters.Add(p5);

                    var p6 = cmd.CreateParameter();
                    p6.ParameterName = "6";
                    p6.DbType = DbType.String;
                    p6.Value = "str3";
                    cmd.Parameters.Add(p6);

                    int count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(3, count);
                }

                conn.Close();
            }
        }

        [Test]
        public void testWithAllQueryTypes()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select 1;" +
                                      "create or replace temporary table t1(c1 varchar);" +
                                      "explain using tabular select * from t1;" +
                                      "show parameters;" +
                                      "insert into t1 values ('str1');" +
                                      "desc table t1;" +
                                      "list @%t1;" +
                                      "remove @%t1;" +
                                      "create or replace temporary procedure P1() returns varchar language javascript as $$ return ''; $$;" +
                                      "call p1();" +
                                      "use role public";

                    // Set statement count
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 11;
                    cmd.Parameters.Add(stmtCountParam);

                    DbDataReader reader = cmd.ExecuteReader();

                    // result of select
                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(-1, reader.RecordsAffected);

                    // result of create
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of explain
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    // server used to return query type of explain as select
                    // but now it could be a specific type of explain
                    Assert.IsTrue((reader.RecordsAffected == 0) ||
                                  (reader.RecordsAffected == -1));

                    // result of show
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of insert
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(1, reader.RecordsAffected);

                    // result of describe
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of list
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of remove
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of create
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    // result of call
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.HasRows);
                    // The server behaivor is inconsistant for now, some of
                    // them returns procedure call as select while some of
                    // them use the new type.
                    Assert.IsTrue((reader.RecordsAffected == 0) ||
                                  (reader.RecordsAffected == -1));

                    // result of use
                    Assert.IsTrue(reader.NextResult());
                    Assert.IsFalse(reader.HasRows);
                    Assert.AreEqual(0, reader.RecordsAffected);

                    Assert.IsFalse(reader.NextResult());
                    reader.Close();
                }

                conn.Close();
            }
        }

        [Test]
        public void testWithMultipleStatementSetting()
        {
            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (DbCommand cmd = conn.CreateCommand())
                {
                    // MULTI_STATEMENT_COUNT=1
                    // multiple statements execution is disabled
                    cmd.CommandText = "alter session set MULTI_STATEMENT_COUNT=1";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "select 1; select 2; select 3";
                    try
                    {
                        cmd.ExecuteNonQuery();
                        Assert.Fail();
                    }
                    catch
                    {
                        // For now don't check the error since the error message
                        // is different between product server and test server
                    }

                    // MULTI_STATEMENT_COUNT=0
                    // multiple statements execution is enabled
                    cmd.CommandText = "alter session set MULTI_STATEMENT_COUNT=0";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "select 1; select 2; select 3";
                    cmd.ExecuteNonQuery();

                    // Set MULTI_STATEMENT_COUNT per query (not match)
                    var stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 4;
                    cmd.Parameters.Add(stmtCountParam);

                    cmd.CommandText = "select 1; select 2; select 3";
                    try
                    {
                        cmd.ExecuteNonQuery();
                        Assert.Fail();
                    }
                    catch
                    {
                        // For now don't check the error since the error message
                        // is different between product server and test server
                    }

                    // Set MULTI_STATEMENT_COUNT per query (match)
                    cmd.Parameters.Clear();
                    stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 3;
                    cmd.Parameters.Add(stmtCountParam);

                    cmd.CommandText = "select 1; select 2; select 3";
                    cmd.ExecuteNonQuery();

                    // No matter how session paramter is set
                    // parameter per query always works
                    // MULTI_STATEMENT_COUNT=0
                    // multiple statements execution is enabled
                    cmd.Parameters.Clear();
                    cmd.CommandText = "alter session set MULTI_STATEMENT_COUNT=1";
                    cmd.ExecuteNonQuery();

                    stmtCountParam = cmd.CreateParameter();
                    stmtCountParam.ParameterName = "MULTI_STATEMENT_COUNT";
                    stmtCountParam.DbType = DbType.Int16;
                    stmtCountParam.Value = 3;
                    cmd.Parameters.Add(stmtCountParam);

                    cmd.CommandText = "select 1; select 2; select 3";
                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            }
        }
    }
}
