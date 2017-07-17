using System;
using System.Collections.Generic;
using System.Data;

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Client;
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

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numDouble, reader.GetDouble(0));

                Assert.IsFalse(reader.Read());

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

                // For time, we getDateTime on the column and ignore date part
                DateTime actualTime = reader.GetDateTime(1);
                Assert.AreEqual(now.Ticks - now.Date.Ticks, actualTime.Ticks - actualTime.Date.Ticks);

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

                cmd.CommandText = "drop table if exists testgettimestampntz";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }

        }
    }
}
