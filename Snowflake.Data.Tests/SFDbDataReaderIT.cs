/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using NUnit.Framework;

    [TestFixture]
    class SFDbDataReaderIT : SFBaseTest
    {
        static private readonly Random rand = new Random();

        [Test]
        public void testGetNumber()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetNumber(cola number)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                int numInt = 10000;
                long numLong = 1000000L;
                short numShort = 10;

                string insertCommand = "insert into testgetnumber values (?),(?),(?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = numInt;
                p1.DbType = DbType.Int32;
                cmd.Parameters.Add(p1);

                var p2 = cmd.CreateParameter();
                p2.ParameterName = "2";
                p2.Value = numLong;
                p2.DbType = DbType.Int32;
                cmd.Parameters.Add(p2);

                var p3 = cmd.CreateParameter();
                p3.ParameterName = "3";
                p3.Value = numShort;
                p3.DbType = DbType.Int16;
                cmd.Parameters.Add(p3);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(3, count);

                cmd.CommandText = "select * from testgetnumber";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numInt, reader.GetInt32(0));

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numLong, reader.GetInt64(0));

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numShort, reader.GetInt16(0));

                Assert.IsFalse(reader.Read());
                reader.Close();

                cmd.CommandText = "drop table if exists testgetnumber";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }

        }

        [Test]
        public void testGetFloat()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetDouble(cola double)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                float numFloat = (float)1.23;
                double numDouble = (double)1.2345678;

                string insertCommand = "insert into testgetdouble values (?),(?)" ;
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = numFloat;
                p1.DbType = DbType.Double;
                cmd.Parameters.Add(p1);

                var p2 = cmd.CreateParameter();
                p2.ParameterName = "2";
                p2.Value = numDouble;
                p2.DbType = DbType.Double;
                cmd.Parameters.Add(p2);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(2, count);

                cmd.CommandText = "select * from testgetdouble";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numFloat, reader.GetFloat(0));
                Assert.AreEqual((decimal)numFloat, reader.GetDecimal(0));


                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numDouble, reader.GetDouble(0));

                Assert.IsFalse(reader.Read());
                reader.Close();

                cmd.CommandText = "drop table if exists testgetdouble";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetDateTime()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetDateTime(cola date, colb time)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                DateTime today = DateTime.Today;
                DateTime now = DateTime.Now;

                string insertCommand = "insert into testgetdatetime values (?, ?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = today;
                p1.DbType = DbType.Date;
                cmd.Parameters.Add(p1);

                var p2 = cmd.CreateParameter();
                p2.ParameterName = "2";
                p2.Value = now;
                p2.DbType = DbType.Time;
                cmd.Parameters.Add(p2);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgetdatetime";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(0, DateTime.Compare(today, reader.GetDateTime(0)));
                Assert.AreEqual(today.ToString("yyyy-MM-dd"), reader.GetString(0));

                // For time, we getDateTime on the column and ignore date part
                DateTime actualTime = reader.GetDateTime(1);
                Assert.AreEqual(now.Ticks - now.Date.Ticks, actualTime.Ticks - actualTime.Date.Ticks);
                reader.Close();

                cmd.CommandText = "drop table if exists testgetdatetime";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetTimestampNTZ()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetTimestampNTZ(cola timestamp_ntz)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                DateTime now = DateTime.Now;

                string insertCommand = "insert into testgettimestampntz values (?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = now;
                p1.DbType = DbType.DateTime;
                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgettimestampntz";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(0, DateTime.Compare(now, reader.GetDateTime(0)));
                reader.Close();

                cmd.CommandText = "drop table if exists testgettimestampntz";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetTimestampTZ()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetTimestampTZ(cola timestamp_tz)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                DateTimeOffset now = DateTimeOffset.Now;

                string insertCommand = "insert into testgettimestamptz values (?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = now;
                p1.DbType = DbType.DateTimeOffset;
                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgettimestamptz";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                DateTimeOffset dtOffset = (DateTimeOffset)reader.GetValue(0);
                reader.Close();

                Assert.AreEqual(0, DateTimeOffset.Compare(now, dtOffset));

                cmd.CommandText = "drop table if exists testgettimestamptz";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }

        }

        [Test]
        public void testGetTimestampLTZ()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetTimestampLTZ(cola timestamp_ltz)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                DateTimeOffset now = DateTimeOffset.Now;

                string insertCommand = "insert into testgettimestampltz values (?)";
                cmd.CommandText = insertCommand;

                var p1 = (SnowflakeDbParameter)cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = now;
                p1.DbType = DbType.DateTimeOffset;
                p1.SFDataType = Core.SFDataType.TIMESTAMP_LTZ;
                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgettimestampltz";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                DateTimeOffset dtOffset = (DateTimeOffset)reader.GetValue(0);
                reader.Close();

                Assert.AreEqual(0, DateTimeOffset.Compare(now, dtOffset));

                cmd.CommandText = "drop table if exists testgettimestamptz";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetBoolean()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetBoolean(cola boolean)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string insertCommand = "insert into testgetboolean values (?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Boolean;
                p1.Value = true;
                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgetboolean";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.IsTrue(reader.GetBoolean(0));
                reader.Close();

                cmd.CommandText = "drop table if exists testgetboolean";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetBinary()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetBinary(cola binary)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string insertCommand = "insert into testgetbinary values (?)";
                cmd.CommandText = insertCommand;
                
                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARAY");

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes; 
                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgetbinary";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.IsTrue(testBytes.SequenceEqual((byte[])reader.GetValue(0)));
                reader.Close();

                cmd.CommandText = "drop table if exists testgetbinary";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetValueIndexOutOfBound()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());

                try
                {
                    reader.GetInt16(-1);
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(270002, e.ErrorCode);
                }

                try
                {
                    reader.GetInt16(1);
                    Assert.Fail();
                }
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(270002, e.ErrorCode);
                }
                reader.Close();

                conn.Close();
            }
        }

        [Test]
        public void testBasicDataReader()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select 1 as colone, 2 as coltwo";
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.AreEqual(2, reader.FieldCount);
                        Assert.AreEqual(0, reader.Depth);
                        Assert.IsTrue(((SnowflakeDbDataReader)reader).HasRows);
                        Assert.IsFalse(reader.IsClosed);
                        Assert.AreEqual("COLONE", reader.GetName(0));
                        Assert.AreEqual("COLTWO", reader.GetName(1));

                        Assert.AreEqual(typeof(long), reader.GetFieldType(0));
                        Assert.AreEqual(typeof(long), reader.GetFieldType(1));

                        Assert.IsFalse(reader.NextResult());
                        Assert.AreEqual(-1, reader.RecordsAffected);

                        Assert.AreEqual(0, reader.GetOrdinal("COLONE"));
                        // reapet calling to test if cache in memory worked or not
                        Assert.AreEqual(0, reader.GetOrdinal("COLONE"));
                        Assert.AreEqual(0, reader.GetOrdinal("COLONE"));
                        Assert.AreEqual(1, reader.GetOrdinal("COLTWO"));
                        Assert.AreEqual(-1, reader.GetOrdinal("COL_NOT_EXISTS"));

                        reader.Close();
                        Assert.IsTrue(reader.IsClosed);
                        
                        try
                        {
                            reader.Read();
                            Assert.Fail();
                        }
                        catch(SnowflakeDbException e)
                        {
                            Assert.AreEqual(270010, e.ErrorCode);
                        }

                        try
                        {
                            reader.GetInt16(0);
                            Assert.Fail();
                        }
                        catch(SnowflakeDbException e)
                        {
                            Assert.AreEqual(270010, e.ErrorCode);
                        }
                    }
                }

                conn.Close();
            }
        }

        [Test]
        public void testReadOutNullVal()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select null";
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        object nullVal = reader.GetValue(0);
                        Assert.AreEqual(DBNull.Value, nullVal);
                        Assert.IsTrue(reader.IsDBNull(0));

                        reader.Close();
                    }
                }

                conn.Close();
            } 
        }

        [Test]
        public void testGetGuid()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetGuid(cola string)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string insertCommand = "insert into testgetguid values (?)";
                cmd.CommandText = insertCommand;

                Guid val = Guid.NewGuid();

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Guid;
                p1.Value = val;
                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testgetguid";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(val, reader.GetGuid(0));

                // test using [] operator
                Assert.AreEqual(val.ToString(), reader[0]);
                Assert.AreEqual(val.ToString(), reader["COLA"]);

                object[] values = new object[1];
                Assert.AreEqual(1, reader.GetValues(values));
                Assert.AreEqual(val.ToString(), values[0]);

                reader.Close();

                cmd.CommandText = "drop table if exists testgetguid";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }

        }
    }
}
