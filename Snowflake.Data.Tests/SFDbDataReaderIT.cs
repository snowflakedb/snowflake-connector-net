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
    class SFDbDataReaderIT : SFBaseTest
    {
        static private readonly Random rand = new Random();

        [Test]
        public void testRecordsAffected()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testRecordsAffected(cola number)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string insertCommand = "insert into testRecordsAffected values (1),(1),(1)";
                cmd.CommandText = insertCommand;
                IDataReader reader = cmd.ExecuteReader();
                Assert.AreEqual(3, reader.RecordsAffected);

                // Reader's RecordsAffected should be available even if the reader is closed
                reader.Close();
                Assert.AreEqual(3, reader.RecordsAffected);

                cmd.CommandText = "drop table if exists testRecordsAffected";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                // Reader's RecordsAffected should be available even if the connection is closed
                conn.Close();
                Assert.AreEqual(3, reader.RecordsAffected);
            }
        }

        [Test]
        public void testGetNumber()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
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
        [TestCase(null)]
        [TestCase("9999-12-31 00:00:00.0000000")]
        [TestCase("9999-12-30 00:00:00.0000000")]
        [TestCase("1982-01-18 00:00:00.0000000")]
        [TestCase("1969-07-21 00:00:00.0000000")]
        [TestCase("1900-09-03 00:00:00.0000000")]
        public void TestGetDate(string inputTimeStr)
        {
            testGetDateAndOrTime(inputTimeStr, null, SFDataType.DATE);
        }


        [Test]
        [TestCase(null, null)]
        [TestCase(null, 3)]
        [TestCase("9999-12-31 23:59:59.9999999", null)]
        [TestCase("9999-12-31 23:59:59.9999999", 5)]
        [TestCase("1982-01-18 16:20:00.6666666", null)]
        [TestCase("1982-01-18 16:20:00.6666666", 3)]
        [TestCase("1969-07-21 02:56:15.1234567", null)]
        [TestCase("1969-07-21 02:56:15.1234567", 1)]
        [TestCase("1900-09-03 12:12:12.1212121", null)]
        [TestCase("1900-09-03 12:12:12.1212121", 1)]
        public void testGetTime(string inputTimeStr, int? precision)
        {
            testGetDateAndOrTime(inputTimeStr, precision, SFDataType.TIME);
        }

        [Test]
        [TestCase("11:22:33.4455667")]
        [TestCase("23:59:59.9999999")]
        [TestCase("16:20:00.6666666")]
        [TestCase("00:00:00.0000000")] 
        [TestCase("00:00:00")]
        [TestCase("23:59:59.1")]
        [TestCase("23:59:59.12")]
        [TestCase("23:59:59.123")]
        [TestCase("23:59:59.1234")]
        [TestCase("23:59:59.12345")]
        [TestCase("23:59:59.123456")]
        [TestCase("23:59:59.1234567")]
        [TestCase("23:59:59.12345678")]
        [TestCase("23:59:59.123456789")]
        public void testGetTimeSpan(string inputTimeStr)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Insert data
                int fractionalPartIndex = inputTimeStr.IndexOf('.');
                var precision = fractionalPartIndex > 0 ? inputTimeStr.Length - (inputTimeStr.IndexOf('.') + 1) : 0;
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"create or replace table testGetTimeSpan(cola TIME{ (precision > 0 ? string.Empty : $"({precision})")});";
                int count = cmd.ExecuteNonQuery();

                string insertCommand = $"insert into testGetTimeSpan values ('{inputTimeStr}')";
                cmd.CommandText = insertCommand;
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "SELECT cola FROM testGetTimeSpan";
                IDataReader reader = cmd.ExecuteReader();

                Assert.IsTrue(reader.Read());

                // For time, we getDateTime on the column and ignore date part
                DateTime dateTimeTime = reader.GetDateTime(0);
                TimeSpan timeSpanTime = ((SnowflakeDbDataReader)reader).GetTimeSpan(0);
                reader.Close();

                // The expected result. Timespan precision only goes up to 7 digits
                TimeSpan expected = TimeSpan.ParseExact(inputTimeStr.Length < 16 ? inputTimeStr : inputTimeStr.Substring(0, 16), "c", CultureInfo.InvariantCulture);
                // Verify the result
                Assert.AreEqual(expected, timeSpanTime);
                Assert.AreEqual(dateTimeTime.Hour, timeSpanTime.Hours);
                Assert.AreEqual(dateTimeTime.Minute, timeSpanTime.Minutes);
                Assert.AreEqual(dateTimeTime.Second, timeSpanTime.Seconds);
                Assert.AreEqual(dateTimeTime.Millisecond, timeSpanTime.Milliseconds);

                cmd.CommandText = "drop table if exists testGetTimeSpan";
                count = cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        [Test]
        public void testGetTimeSpanError()
        {
            // Only Time data can be retrieved using GetTimeSpan, other type will fail
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Insert data
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetTimeSpanAllDatTypes(C1 NUMBER(38,0), C2 FLOAT, C3 VARCHAR(255), C4 BINARY(255), C5 BOOLEAN," +
                    " C6 DATE, C7 TIMESTAMP_NTZ(9), C8 TIMESTAMP_LTZ(9), C9 TIMESTAMP_TZ(9), " +
                    "C10 VARIANT, C11 OBJECT, C12 ARRAY, C13 VARCHAR(1), C14 TIME); ";
                //Console.WriteLine(cmd.CommandText);
                int count = cmd.ExecuteNonQuery();

                string insertCommand = "insert into testGetTimeSpanAllDatTypes(C1, C10, C11, C12) select 1, "+
                "PARSE_JSON('{ \"key1\": \"value1\", \"key2\": \"value2\" }')" +
                 ", PARSE_JSON(' { \"outer_key1\": { \"inner_key1A\": \"1a\", \"inner_key1B\": NULL }, '||' \"outer_key2\": { \"inner_key2\": 2 } '||' } ')," +
                 " ARRAY_CONSTRUCT(1, 2, 3, NULL)";
                cmd.CommandText = insertCommand;
                //Console.WriteLine(insertCommand);
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                insertCommand = "update testGetTimeSpanAllDatTypes set C2 = 2.5, C3 = 'C3Val', C4 = TO_BINARY('C4'), C5 = true, C6 = '2021-01-01', " +
                "C7 = '2017-01-01 12:00:00', C8 = '2017-01-01 12:00:00 +04:00', C9 = '2014-01-02 16:00:00 +10:00', C14 = '12:00:00' where C1 = 1";
                cmd.CommandText = insertCommand;
                //Console.WriteLine(insertCommand);
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "SELECT * FROM testGetTimeSpanAllDatTypes";
                IDataReader reader = cmd.ExecuteReader();

                Assert.IsTrue(reader.Read());

                // All types except TIME fail conversion when calling GetTimeSpan
                for (int i = 0; i < 12; i++)
                {
                    try
                    {
                        
                        ((SnowflakeDbDataReader)reader).GetTimeSpan(i);
                        Assert.Fail("Data should not be converted to TIME");
                    }
                    catch (SnowflakeDbException e)
                    {
                        Assert.AreEqual(270003, e.ErrorCode); 
                    }
                }

                // Null value
                // Null value can not be converted to TimeSpan because it is a non-nullable type
                
                try
                {
                    ((SnowflakeDbDataReader)reader).GetTimeSpan(12);
                    Assert.Fail("TimeSpan is not nullable");
                }
                catch (InvalidCastException)
                {
                    // Expected, ignore it
                }

                // Valid time column
                TimeSpan timeSpanTime = ((SnowflakeDbDataReader)reader).GetTimeSpan(13);

                reader.Close();
                
                cmd.CommandText = "drop table if exists testGetTimeSpanAllDatTypes";
                count = cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        private void testGetDateAndOrTime(string inputTimeStr, int? precision, SFDataType dataType)
        {
            // Can't use DateTime object as test case, must parse.
            DateTime inputTime;
            if (inputTimeStr == null)
            {
                inputTime = dataType == SFDataType.DATE ? DateTime.Today : DateTime.Now;
            }
            else
            {
                inputTime = DateTime.ParseExact(inputTimeStr, "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"create or replace table testGetDateAndOrTime(cola {dataType}{ (precision == null ? string.Empty : $"({precision})" )});";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string insertCommand = "insert into testGetDateAndOrTime values (?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = inputTime;
                switch (dataType)
                {
                    case SFDataType.TIME:
                        p1.DbType = DbType.Time;
                        break;
                    case SFDataType.DATE:
                        p1.DbType = DbType.Date;
                        break;
                    case SFDataType.TIMESTAMP_LTZ:
                    case SFDataType.TIMESTAMP_TZ:
                    case SFDataType.TIMESTAMP_NTZ:
                        p1.DbType = DbType.DateTime;
                        break;
                }

                cmd.Parameters.Add(p1);

                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testGetDateAndOrTime";
                IDataReader reader = cmd.ExecuteReader();
                
                Assert.IsTrue(reader.Read());

                // For time, we getDateTime on the column and ignore date part
                DateTime actualTime = reader.GetDateTime(0);

                if (dataType == SFDataType.DATE)
                {
                    Assert.AreEqual(inputTime.Date, reader.GetDateTime(0));
                    Assert.AreEqual(inputTime.Date.ToString("yyyy-MM-dd"), reader.GetString(0));
                }
                if (dataType != SFDataType.DATE)
                {
                    var inputTimeTicksOfTheDay = inputTime.Ticks - inputTime.Date.Ticks;
                    var actualTimeTicksOfTheDay = actualTime.Ticks - actualTime.Date.Ticks;
                    var allowedPrecisionLossInTicks = precision < 7 ? Math.Pow(10, (double)(7 - precision)) - 1 : 0d;
                    Assert.AreEqual(inputTimeTicksOfTheDay, actualTimeTicksOfTheDay, allowedPrecisionLossInTicks);
                }
                if (dataType == SFDataType.TIMESTAMP_NTZ)
                {
                    if (precision == 9)
                    {
                        Assert.AreEqual(inputTime, reader.GetDateTime(0));
                    }
                    else
                    {
                        Assert.AreEqual(inputTime.Date, reader.GetDateTime(0).Date);
                    }
                }

                reader.Close();

                cmd.CommandText = "drop table if exists testGetDateAndOrTime";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        [TestCase(null, null)]
        [TestCase(null, 3)]
        [TestCase("2100-12-31 23:59:59.9999999", null)]
        [TestCase("2100-12-31 23:59:59.9999999", 5)]
        [TestCase("9999-12-31 23:59:59.9999999", null)]
        [TestCase("9999-12-31 23:59:59.9999999", 5)]
        [TestCase("9999-12-30 23:59:59.9999999", null)]
        [TestCase("9999-12-30 23:59:59.9999999", 5)]
        [TestCase("1982-01-18 16:20:00.6666666", null)]
        [TestCase("1982-01-18 16:20:00.6666666", 3)]
        //[TestCase("1969-07-21 02:56:15.1234567", null)] //parsing fails with dates with second fractions before the unix epoch
        [TestCase("1969-07-21 02:56:15.0000000", 1)] //dates w/o second fractions before the unix epoch are fine
        //[TestCase("1900-09-03 12:12:12.1212121", null)] // fails
        [TestCase("1900-09-03 12:12:12.0000000", 1)]
        public void testGetTimestampNTZ(string inputTimeStr, int? precision)
        {
            testGetDateAndOrTime(inputTimeStr, precision, SFDataType.TIMESTAMP_NTZ);
        }


        [Test]
        public void testGetTimestampTZ()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetBinary(col1 binary, col2  VARCHAR(50), col3 double)";
                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARAY");
                string testChars = "TEST_GET_CHARS";
                double testDouble = 1.2345678;
                string insertCommand = $"insert into testgetbinary values (?, '{testChars}',{testDouble.ToString()})";
                cmd.CommandText = insertCommand;
                
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
                // Auto type conversion
                Assert.IsTrue(testBytes.SequenceEqual((byte[])reader.GetValue(0)));
                Assert.IsTrue(testChars.Equals(reader.GetValue(1)));
                Assert.IsTrue(testDouble.Equals(reader.GetValue(2)));

                // Read all 'TEST_GET_BINARAY' data
                int toReadLength = testBytes.Length;
                byte[] sub = new byte[toReadLength];
                long read = reader.GetBytes(0, 0, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testBytes.SequenceEqual(sub));

                // Read subset 'GET_BINARAY' from actual 'TEST_GET_BINARAY' data
                toReadLength = 11;
                byte[] testSubBytes = Encoding.UTF8.GetBytes("GET_BINARAY");
                sub = new byte[toReadLength];
                read = reader.GetBytes(0, 5, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubBytes.SequenceEqual(sub));

                // Read subset 'GET_CHARS' from actual 'TEST_GET_CHARS' data
                toReadLength = 9;
                testSubBytes = Encoding.UTF8.GetBytes("GET_CHARS");
                sub = new byte[toReadLength];
                read = reader.GetBytes(1, 5, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubBytes.SequenceEqual(sub));

                // Read subset '5678' from actual '1.2345678' data
                toReadLength = 4;
                testSubBytes = Encoding.UTF8.GetBytes("5678");
                sub = new byte[toReadLength];
                read = reader.GetBytes(2, 5, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubBytes.SequenceEqual(sub));

                // Read subset 'GET_BINARAY'  from actual 'TEST_GET_BINARAY' data 
                // and copy inside existing buffer replacing Xs
                toReadLength = 11;
                byte[] testSubBytesWithTargetOffset = Encoding.UTF8.GetBytes("OFFSET GET_BINARAY EXTRA");
                sub = Encoding.UTF8.GetBytes("OFFSET XXXXXXXXXXX EXTRA");
                read = reader.GetBytes(0, 5, sub, 7, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubBytesWithTargetOffset.SequenceEqual(sub));

                // Less data than 'ask' for
                int dataOffset = 10;
                read = reader.GetBytes(0, dataOffset, sub, 0, toReadLength);
                Assert.AreEqual(read, testBytes.Length - dataOffset);

                //** Invalid data offsets **/
                try
                {
                    // Data offset > data length 
                    reader.GetBytes(0, 25, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("dataOffset", e.ParamName);
                }

                try
                {
                    // Data offset < 0
                    reader.GetBytes(0, -1, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("dataOffset", e.ParamName);
                }

                //** Invalid buffer offsets **//
                try
                {
                    // Buffer offset > buffer length 
                    reader.GetBytes(0, 6, sub, 25, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("buffer", e.ParamName);
                }

                try
                {
                    // Buffer offset < 0
                    reader.GetBytes(0, 6, sub, -1, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("bufferOffset", e.ParamName);
                }

                //** Null buffer **//
                // If null, this method returns the size required of the array in order to fit all 
                // of the specified data.
                read = reader.GetBytes(0, 6, null, 0, toReadLength);
                Assert.AreEqual(testBytes.Length, read);

                reader.Close();

                cmd.CommandText = "drop table if exists testgetbinary";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetChars()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetChars(col1 VARCHAR(50), col2 binary, col3 double)";

                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string testChars = "TEST_GET_CHARS";
                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARY");
                double testDouble = 1.2345678;
                cmd.CommandText = $"insert into testGetChars values ('{testChars}', ?, {testDouble.ToString()})";

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes;
                cmd.Parameters.Add(p1);


                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testGetChars";
                IDataReader reader = cmd.ExecuteReader();

                Assert.IsTrue(reader.Read());
                // Auto type conversion
                Assert.IsTrue(testChars.Equals(reader.GetValue(0)));
                Assert.IsTrue(testBytes.SequenceEqual((byte[])reader.GetValue(1)));
                Assert.IsTrue(testDouble.Equals(reader.GetValue(2)));

                // Read all 'TEST_GET_CHARS' data
                int toReadLength = 14;
                char[] testSubChars = testChars.ToArray<char>();
                char[] sub = new char[toReadLength];
                long read = reader.GetChars(0, 0, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubChars.SequenceEqual(sub));

                // Read subset 'GET_CHARS' from actual 'TEST_GET_CHARS' data
                toReadLength = 9;
                testSubChars = "GET_CHARS".ToArray<char>();
                sub = new char[toReadLength];
                read = reader.GetChars(0, 5, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubChars.SequenceEqual(sub));

                // Read subset 'GET_BINARY' from actual 'TEST_GET_BINARY' data
                toReadLength = 10;
                testSubChars = "GET_BINARY".ToArray<char>();
                sub = new char[toReadLength];
                read = reader.GetChars(1, 5, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubChars.SequenceEqual(sub));

                // Read subset '5678' from actual '1.2345678' data
                toReadLength = 4;
                testSubChars = "5678".ToArray<char>();
                sub = new char[toReadLength];
                read = reader.GetChars(2, 5, sub, 0, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubChars.SequenceEqual(sub));


                // Read subset 'GET_CHARS'  from actual 'TEST_GET_CHARS' data 
                // and copy inside existing buffer replacing Xs
                char[] testSubCharsWithTargetOffset = "OFFSET GET_CHARS EXTRA".ToArray<char>();
                toReadLength = 9;
                sub = "OFFSET XXXXXXXXX EXTRA".ToArray<char>();
                read = reader.GetChars(0, 5, sub, 7, toReadLength);
                Assert.AreEqual(read, toReadLength);
                Assert.IsTrue(testSubCharsWithTargetOffset.SequenceEqual(sub));

                // Less data than 'ask' for
                int dataOffset = 10;
                read = reader.GetChars(0, dataOffset, sub, 0, toReadLength);
                Assert.AreEqual(read, testChars.Length - dataOffset);

                //** Invalid data offsets **//
                try
                {
                    // Data offset > data length 
                    reader.GetChars(0, 25, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("dataOffset", e.ParamName);
                }

                try
                {
                    // Data offset < 0
                    reader.GetChars(0, -1, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("dataOffset", e.ParamName);
                }

                //** Invalid buffer offsets **//
                try
                {
                    // Buffer offset > buffer length 
                    reader.GetChars(0, 6, sub, 25, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("buffer", e.ParamName);
                }

                try
                {
                    // Buffer offset < 0
                    reader.GetChars(0, 6, sub, -1, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("bufferOffset", e.ParamName);
                }

                //** Null buffer **//
                // If null, this method returns the size required of the array in order to fit all 
                // of the specified data.
                read = reader.GetChars(0, 6, null, 0, toReadLength);
                Assert.AreEqual(testChars.Length, read);

                reader.Close();

                cmd.CommandText = "drop table if exists testGetChars";
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                conn.Close();
            }
        }

        [Test]
        public void testGetStream()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testGetChars(col1 VARCHAR(50), col2 binary, col3 double)";

                int count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                string testChars = "TEST_GET_CHARS";
                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARY");
                double testDouble = 1.2345678;
                cmd.CommandText = $"insert into testGetChars values ('{testChars}', ?, {testDouble.ToString()})";

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes;
                cmd.Parameters.Add(p1);


                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = "select * from testGetChars";
                DbDataReader reader = (DbDataReader) cmd.ExecuteReader();

                Assert.IsTrue(reader.Read());

                // Auto type conversion
                Assert.IsTrue(testChars.Equals(reader.GetValue(0)));
                Assert.IsTrue(testBytes.SequenceEqual((byte[])reader.GetValue(1)));
                Assert.IsTrue(testDouble.Equals(reader.GetValue(2)));

                using (var stream = reader.GetStream(0))
                {
                    byte[] col1ToBytes = Encoding.UTF8.GetBytes(testChars);
                    byte[] buf = new byte[col1ToBytes.Length];
                    stream.Read(buf, 0, col1ToBytes.Length);
                    Assert.IsTrue(-1 == stream.ReadByte()); // No more data
                    Assert.IsTrue(col1ToBytes.SequenceEqual(buf));
                }

                using (var stream = reader.GetStream(1))
                {
                    byte[] buf = new byte[testBytes.Length];
                    stream.Read(buf, 0, testBytes.Length);
                    Assert.IsTrue(-1 == stream.ReadByte()); // No more data
                    Assert.IsTrue(testBytes.SequenceEqual(buf));
                }

                using (var stream = reader.GetStream(2))
                {
                    byte[] col3ToBytes = Encoding.UTF8.GetBytes(testDouble.ToString());
                    byte[] buf = new byte[col3ToBytes.Length];
                    stream.Read(buf, 0, col3ToBytes.Length);
                    Assert.IsTrue(-1 == stream.ReadByte()); // No more data
                    Assert.IsTrue(col3ToBytes.SequenceEqual(buf));
                }


                reader.Close();

                cmd.CommandText = "drop table if exists testGetChars";
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
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "create or replace table testnull(a integer, b string)";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "insert into testnull values(null, null)";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "select * from testnull";
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        object nullVal = reader.GetValue(0);
                        Assert.AreEqual(DBNull.Value, nullVal);
                        Assert.IsTrue(reader.IsDBNull(0));
                        Assert.IsTrue(reader.IsDBNull(1));

                        reader.Close();
                    }

                    cmd.CommandText = "drop table if exists testnull";
                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            } 
        }

        [Test]
        public void testGetGuid()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
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

        [Test]
        public void TestCopyCmdUpdateCount()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace stage emptyStage";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "create or replace table testCopy (cola string)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "copy into testCopy from @emptyStage";
                int updateCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, updateCount);

                // test rows_loaded exists
                cmd.CommandText = "copy into @%testcopy from (select 'test_string')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "copy into testcopy";
                updateCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, updateCount);

                // clean up
                cmd.CommandText = "drop stage emptyStage";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "drop table testCopy";
                cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        [Test]
        public void TestCopyCmdResultSet()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace stage emptyStage";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "create or replace table testCopy (cola string)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "copy into testCopy from @emptyStage";                
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.AreEqual(true, rdr.Read());
                }

                // test rows_loaded exists
                cmd.CommandText = "copy into @%testcopy from (select 'test_string')";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.AreEqual(true, rdr.Read());
                }

                cmd.CommandText = "copy into testcopy";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.AreEqual(true, rdr.Read());
                }

                // clean up
                cmd.CommandText = "drop stage emptyStage";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "drop table testCopy";
                cmd.ExecuteNonQuery();

                conn.Close();
            }
        }

        [Test]
        public void TestRetrieveSemiStructuredData()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table testsemi(cola variant, colb array, colc object) " +
                    "as select '[\"1\", \"2\"]', '[\"1\", \"2\"]', '{\"key\": \"value\"}'";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "select * from testsemi";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(true, reader.Read());
                    Assert.AreEqual("[\n  \"1\",\n  \"2\"\n]", reader.GetString(0));
                    Assert.AreEqual("[\n  \"1\",\n  \"2\"\n]", reader.GetString(1));
                    Assert.AreEqual("{\n  \"key\": \"value\"\n}", reader.GetString(2));
                }

                conn.Close();
            }
        }

        [Test]
        public void TestResultSetMetadata()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create or replace table meta(c1 number(20, 4), c2 string(100), " +
                    "c3 double, c4 timestamp_ntz, c5 variant not null, c6 boolean) ";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "select * from meta";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    var dataTable = reader.GetSchemaTable();
                    dataTable.DefaultView.Sort = SchemaTableColumn.ColumnName;
                    dataTable = dataTable.DefaultView.ToTable();

                    DataRow row = dataTable.Rows[0];
                    Assert.AreEqual("C1", row[SchemaTableColumn.ColumnName]);
                    Assert.AreEqual(0, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.AreEqual(20, row[SchemaTableColumn.NumericPrecision]);
                    Assert.AreEqual(4, row[SchemaTableColumn.NumericScale]);
                    Assert.AreEqual(SFDataType.FIXED, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.AreEqual(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[1];
                    Assert.AreEqual("C2", row[SchemaTableColumn.ColumnName]);
                    Assert.AreEqual(1, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.AreEqual(100, row[SchemaTableColumn.ColumnSize]);
                    Assert.AreEqual(SFDataType.TEXT, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.AreEqual(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[2];
                    Assert.AreEqual("C3", row[SchemaTableColumn.ColumnName]);
                    Assert.AreEqual(2, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.AreEqual(SFDataType.REAL, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.AreEqual(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[3];
                    Assert.AreEqual("C4", row[SchemaTableColumn.ColumnName]);
                    Assert.AreEqual(3, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.AreEqual(0, row[SchemaTableColumn.NumericPrecision]);
                    Assert.AreEqual(9, row[SchemaTableColumn.NumericScale]);
                    Assert.AreEqual(SFDataType.TIMESTAMP_NTZ, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.AreEqual(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[4];
                    Assert.AreEqual("C5", row[SchemaTableColumn.ColumnName]);
                    Assert.AreEqual(4, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.AreEqual(SFDataType.VARIANT, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.AreEqual(false, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[5];
                    Assert.AreEqual("C6", row[SchemaTableColumn.ColumnName]);
                    Assert.AreEqual(5, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.AreEqual(SFDataType.BOOLEAN, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.AreEqual(true, row[SchemaTableColumn.AllowDBNull]);
                }

                conn.Close();
            }
        }
    }
}