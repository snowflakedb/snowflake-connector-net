using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture(ResultFormat.ARROW)]
    [TestFixture(ResultFormat.JSON)]
    class SFDbDataReaderIT : SFBaseTest
    {
        protected override string TestName => base.TestName + _resultFormat;

        private readonly ResultFormat _resultFormat;

        public SFDbDataReaderIT(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }

        private void ValidateResultFormat(IDataReader reader)
        {
            Assert.AreEqual(_resultFormat, ((SnowflakeDbDataReader)reader).ResultFormat);
        }

        [Test]
        public void TestRecordsAffected()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER" });

                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName} values (1),(1),(1)";
                cmd.CommandText = insertCommand;
                IDataReader reader = cmd.ExecuteReader();
                Assert.AreEqual(3, reader.RecordsAffected);

                // Reader's RecordsAffected should be available even if the reader is closed
                reader.Close();
                Assert.AreEqual(3, reader.RecordsAffected);

                cmd.CommandText = $"drop table if exists {TableName}";
                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, count);

                // Reader's RecordsAffected should be available even if the connection is closed
                CloseConnection(conn);
                Assert.AreEqual(3, reader.RecordsAffected);
            }
        }

        [Test]
        public void TestGetNumber()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER" });

                IDbCommand cmd = conn.CreateCommand();

                int numInt = 10000;
                long numLong = 1000000L;
                short numShort = 10;

                string insertCommand = $"insert into {TableName} values (?),(?),(?)";
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

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(3, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numInt, reader.GetInt32(0));

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numLong, reader.GetInt64(0));

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numShort, reader.GetInt16(0));

                Assert.IsFalse(reader.Read());
                reader.Close();

                CloseConnection(conn);
            }

        }

        [Test]
        public void TestGetDouble()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola DOUBLE" });

                IDbCommand cmd = conn.CreateCommand();

                float numFloat = (float)1.23;
                double numDouble = (double)1.2345678;

                string insertCommand = $"insert into {TableName} values (?),(?)";
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

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(2, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numFloat, reader.GetFloat(0));
                Assert.AreEqual((decimal)numFloat, reader.GetDecimal(0));


                Assert.IsTrue(reader.Read());
                Assert.AreEqual(numDouble, reader.GetDouble(0));

                Assert.IsFalse(reader.Read());
                reader.Close();

                CloseConnection(conn);
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
            TestGetDateAndOrTime(inputTimeStr, null, SFDataType.DATE);
        }

        [Test]
        public void TestDateOutputFormat()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                IDbCommand cmd = conn.CreateCommand();

                try
                {
                    cmd.CommandText = "alter session set DATE_OUTPUT_FORMAT='MM/DD/YYYY'";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"select TO_DATE('2013-05-17')";
                    IDataReader reader = cmd.ExecuteReader();

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("05/17/2013", reader.GetString(0));

                    reader.Close();
                }
                finally
                {
                    // set format back to default to avoid impact other test cases
                    cmd.CommandText = "alter session set DATE_OUTPUT_FORMAT='YYYY-MM-DD'";
                    cmd.ExecuteNonQuery();
                }

                conn.Close();
            }
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
        public void TestGetTime(string inputTimeStr, int? precision)
        {
            TestGetDateAndOrTime(inputTimeStr, precision, SFDataType.TIME);
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
        public void TestGetTimeSpan(string inputTimeStr)
        {
            using (var conn = CreateAndOpenConnection())
            {
                // Insert data
                int fractionalPartIndex = inputTimeStr.IndexOf('.');
                var precision = fractionalPartIndex > 0 ? inputTimeStr.Length - (inputTimeStr.IndexOf('.') + 1) : 0;
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    $"cola TIME{ (precision > 0 ? string.Empty : $"({precision})")}"
                });
                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName} values ('{inputTimeStr}')";
                cmd.CommandText = insertCommand;
                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"SELECT cola FROM {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

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

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestGetTimeSpanError()
        {
            // Only Time data can be retrieved using GetTimeSpan, other type will fail
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "C1 NUMBER",
                    "C2 FLOAT",
                    "C3 VARCHAR(255)",
                    "C4 BINARY(255)",
                    "C5 BOOLEAN",
                    "C6 DATE",
                    "C7 TIMESTAMP_NTZ(9)",
                    "C8 TIMESTAMP_LTZ(9)",
                    "C9 TIMESTAMP_TZ(9)",
                    "C10 VARIANT",
                    "C11 OBJECT",
                    "C12 ARRAY",
                    "C13 VARCHAR(1)",
                    "C14 TIME"
                });

                // Insert data
                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName}(C1, C10, C11, C12) select 1, " +
                "PARSE_JSON('{ \"key1\": \"value1\", \"key2\": \"value2\" }')" +
                 ", PARSE_JSON(' { \"outer_key1\": { \"inner_key1A\": \"1a\", \"inner_key1B\": NULL }, '||' \"outer_key2\": { \"inner_key2\": 2 } '||' } ')," +
                 " ARRAY_CONSTRUCT(1, 2, 3, NULL)";
                cmd.CommandText = insertCommand;
                //Console.WriteLine(insertCommand);
                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                insertCommand = $"update {TableName} set C2 = 2.5, C3 = 'C3Val', C4 = TO_BINARY('C4'), C5 = true, C6 = '2021-01-01', " +
                "C7 = '2017-01-01 12:00:00', C8 = '2017-01-01 12:00:00 +04:00', C9 = '2014-01-02 16:00:00 +10:00', C14 = '12:00:00' where C1 = 1";
                cmd.CommandText = insertCommand;
                //Console.WriteLine(insertCommand);
                count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"SELECT * FROM {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

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

                CloseConnection(conn);
            }
        }

        private void TestGetDateAndOrTime(string inputTimeStr, int? precision, SFDataType dataType)
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

            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    $"cola {dataType}{ (precision == null ? string.Empty : $"({precision})" )}"
                });

                IDbCommand cmd = conn.CreateCommand();
                string insertCommand = $"insert into {TableName} values (?)";
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

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());

                // For time, we getDateTime on the column and ignore date part
                DateTime actualTime = reader.GetDateTime(0);

                if (dataType == SFDataType.DATE)
                {
                    Assert.AreEqual(inputTime.Date, actualTime);
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
                        Assert.AreEqual(inputTime, actualTime);
                    }
                    else
                    {
                        Assert.AreEqual(inputTime.Date, actualTime.Date);
                    }
                }

                // DATE, TIME and TIMESTAMP_NTZ should be returned with DateTimeKind.Unspecified
                Assert.AreEqual(DateTimeKind.Unspecified, actualTime.Kind);

                reader.Close();

                CloseConnection(conn);
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
        public void TestGetTimestampNTZ(string inputTimeStr, int? precision)
        {
            TestGetDateAndOrTime(inputTimeStr, precision, SFDataType.TIMESTAMP_NTZ);
        }


        [Test]
        [TestCase(0)]
        [TestCase(5)]
        [TestCase(-5)]
        [TestCase(14)]
        [TestCase(-14)]
        public void TestGetTimestampTZ(int timezoneOffsetInHours)
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola TIMESTAMP_TZ" });

                DateTimeOffset now = DateTimeOffset.Now.ToOffset(TimeSpan.FromHours(timezoneOffsetInHours));

                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName} values (?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = now;
                p1.DbType = DbType.DateTimeOffset;
                cmd.Parameters.Add(p1);

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());
                DateTimeOffset dtOffset = (DateTimeOffset)reader.GetValue(0);
                reader.Close();

                Assert.AreEqual(now, dtOffset);
                Assert.AreEqual(now.Offset, dtOffset.Offset);

                CloseConnection(conn);
            }

        }

        [Test]
        public void TestGetTimestampLTZ()
        {
            using (var conn = CreateAndOpenConnectionWithHonorSessionTimezone())
            {
                IDbCommand setTimezoneCmd = conn.CreateCommand();
                setTimezoneCmd.CommandText = "ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'";
                setTimezoneCmd.ExecuteNonQuery();

                CreateOrReplaceTable(conn, TableName, new[] { "cola TIMESTAMP_LTZ" });

                DateTimeOffset insertValue = new DateTimeOffset(2024, 1, 15, 18, 30, 45, 123, TimeSpan.Zero);

                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName} values (?)";
                cmd.CommandText = insertCommand;

                var p1 = (SnowflakeDbParameter)cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.Value = insertValue;
                p1.DbType = DbType.DateTimeOffset;
                p1.SFDataType = Core.SFDataType.TIMESTAMP_LTZ;
                cmd.Parameters.Add(p1);

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());
                DateTimeOffset dtOffset = (DateTimeOffset)reader.GetValue(0);
                reader.Close();

                Assert.AreEqual(insertValue.UtcDateTime, dtOffset.UtcDateTime);

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestGetBoolean([Values] bool value)
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola BOOLEAN" });

                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName} values (?)";
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Boolean;
                p1.Value = value;
                cmd.Parameters.Add(p1);

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(value, reader.GetBoolean(0));
                reader.Close();

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestGetByte()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = ConnectionString;
                conn.Open();

                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "col1 NUMBER(3)",
                });

                short[] testBytes = { 0, 10, 150, 200, 255 };

                IDbCommand cmd = conn.CreateCommand();

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Byte;
                p1.Value = testBytes;

                cmd.Parameters.Add(p1);
                cmd.CommandText = $"insert into {TableName} values (?)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = $"select * from {TableName} order by 1";

                // Act
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    int index = 0;
                    while (reader.Read())
                    {
                        // Assert
                        Assert.AreEqual(testBytes[index++], reader.GetByte(0));
                    }
                }
            }
        }

        [Test]
        public void TestGetBinary()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "col1 BINARY",
                    "col2 VARCHAR(50)",
                    "col3 DOUBLE"
                });

                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARAY");
                string testChars = "TEST_GET_CHARS";
                double testDouble = 1.2345678;
                string insertCommand = $"insert into {TableName} values (?, '{testChars}',{testDouble.ToString()})";
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = insertCommand;

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes;
                cmd.Parameters.Add(p1);

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

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

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestGetChar()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = ConnectionString;
                conn.Open();

                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "col1 VARCHAR(50)",
                });

                char testChar = 'T';

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"insert into {TableName} values ('{testChar}')";
                cmd.ExecuteNonQuery();
                cmd.CommandText = $"select * from {TableName}";

                // Act
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    // Assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(testChar, reader.GetChar(0));
                }
            }
        }

        [Test]
        public void TestGetChars()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "col1 VARCHAR(50)",
                    "col2 BINARY",
                    "col3 DOUBLE"
                });

                string testChars = "TEST_GET_CHARS";
                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARY");
                double testDouble = 1.2345678;
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"insert into {TableName} values ('{testChars}', ?, {testDouble.ToString()})";

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes;
                cmd.Parameters.Add(p1);


                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

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

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestGetDataTypeName()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Arrange
                conn.ConnectionString = ConnectionString;
                conn.Open();

                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "col1 VARCHAR(50)",
                    "col2 BINARY",
                    "col3 DOUBLE"
                });

                string testChars = "TEST_GET_CHARS";
                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARY");
                double testDouble = 1.2345678;

                IDbCommand cmd = conn.CreateCommand();

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes;

                cmd.Parameters.Add(p1);
                cmd.CommandText = $"insert into {TableName} values ('{testChars}', ?, {testDouble.ToString()})";
                cmd.ExecuteNonQuery();
                cmd.CommandText = $"select * from {TableName}";

                // Act
                using (DbDataReader reader = (DbDataReader)cmd.ExecuteReader())
                {
                    // Assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("TEXT", reader.GetDataTypeName(0));
                    Assert.AreEqual("BINARY", reader.GetDataTypeName(1));
                    Assert.AreEqual("REAL", reader.GetDataTypeName(2));
                }
            }
        }

        [Test]
        public void TestGetStream()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "col1 VARCHAR(50)",
                    "col2 BINARY",
                    "col3 DOUBLE"
                });

                string testChars = "TEST_GET_CHARS";
                byte[] testBytes = Encoding.UTF8.GetBytes("TEST_GET_BINARY");
                double testDouble = 1.2345678;
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"insert into {TableName} values ('{testChars}', ?, {testDouble.ToString()})";

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Binary;
                p1.Value = testBytes;
                cmd.Parameters.Add(p1);


                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                DbDataReader reader = (DbDataReader)cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());

                // Auto type conversion
                Assert.IsTrue(testChars.Equals(reader.GetValue(0)));
                Assert.IsTrue(testBytes.SequenceEqual((byte[])reader.GetValue(1)));
                Assert.IsTrue(testDouble.Equals(reader.GetValue(2)));

                using (var stream = reader.GetStream(0))
                {
                    byte[] col1ToBytes = Encoding.UTF8.GetBytes(testChars);
                    byte[] buf = new byte[col1ToBytes.Length];
                    var readBytes = stream.Read(buf, 0, col1ToBytes.Length);
                    Assert.AreEqual(col1ToBytes.Length, readBytes);
                    Assert.IsTrue(-1 == stream.ReadByte()); // No more data
                    Assert.IsTrue(col1ToBytes.SequenceEqual(buf));
                }

                using (var stream = reader.GetStream(1))
                {
                    byte[] buf = new byte[testBytes.Length];
                    var readBytes = stream.Read(buf, 0, testBytes.Length);
                    Assert.AreEqual(testBytes.Length, readBytes);
                    Assert.IsTrue(-1 == stream.ReadByte()); // No more data
                    Assert.IsTrue(testBytes.SequenceEqual(buf));
                }

                using (var stream = reader.GetStream(2))
                {
                    byte[] col3ToBytes = Encoding.UTF8.GetBytes(testDouble.ToString());
                    byte[] buf = new byte[col3ToBytes.Length];
                    var readBytes = stream.Read(buf, 0, col3ToBytes.Length);
                    Assert.AreEqual(col3ToBytes.Length, readBytes);
                    Assert.IsTrue(-1 == stream.ReadByte()); // No more data
                    Assert.IsTrue(col3ToBytes.SequenceEqual(buf));
                }


                reader.Close();

                CloseConnection(conn);
            }
        }


        [Test]
        public void TestGetValueIndexOutOfBound()
        {
            using (var conn = CreateAndOpenConnection())
            {
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());

                try
                {
                    reader.GetInt16(-1);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270002, e.ErrorCode);
                }

                try
                {
                    reader.GetInt16(1);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(270002, e.ErrorCode);
                }
                reader.Close();

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestBasicDataReader()
        {
            using (var conn = CreateAndOpenConnection())
            {
                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select 1 as colone, 2 as coltwo";
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

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
                        catch (SnowflakeDbException e)
                        {
                            Assert.AreEqual(270010, e.ErrorCode);
                        }

                        try
                        {
                            reader.GetInt16(0);
                            Assert.Fail();
                        }
                        catch (SnowflakeDbException e)
                        {
                            Assert.AreEqual(270010, e.ErrorCode);
                        }
                    }
                }

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestReadOutNullVal()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "a INTEGER",
                    "b STRING"
                });

                using (IDbCommand cmd = conn.CreateCommand())
                {

                    cmd.CommandText = $"insert into {TableName} values(null, null)";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"select * from {TableName}";
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);

                        reader.Read();
                        object nullVal = reader.GetValue(0);
                        Assert.AreEqual(DBNull.Value, nullVal);
                        Assert.IsTrue(reader.IsDBNull(0));
                        Assert.IsTrue(reader.IsDBNull(1));

                        reader.Close();
                    }
                }

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestGetGuid()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola STRING" });

                IDbCommand cmd = conn.CreateCommand();
                string insertCommand = $"insert into {TableName} values (?)";
                cmd.CommandText = insertCommand;

                Guid val = Guid.NewGuid();

                var p1 = cmd.CreateParameter();
                p1.ParameterName = "1";
                p1.DbType = DbType.Guid;
                p1.Value = val;
                cmd.Parameters.Add(p1);

                var count = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsTrue(reader.Read());
                Assert.AreEqual(val, reader.GetGuid(0));

                // test using [] operator
                Assert.AreEqual(val.ToString(), reader[0]);
                Assert.AreEqual(val.ToString(), reader["COLA"]);

                object[] values = new object[1];
                Assert.AreEqual(1, reader.GetValues(values));
                Assert.AreEqual(val.ToString(), values[0]);

                reader.Close();

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestCopyCmdUpdateCount()
        {
            var stageName = TestName;
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola STRING" });

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"create or replace stage {stageName}";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"copy into {TableName} from @{stageName}";
                int updateCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, updateCount);

                // test rows_loaded exists
                cmd.CommandText = $"copy into @%{TableName} from (select 'test_string')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"copy into {TableName}";
                updateCount = cmd.ExecuteNonQuery();
                Assert.AreEqual(1, updateCount);

                // clean up
                cmd.CommandText = $"drop stage {stageName}";
                cmd.ExecuteNonQuery();

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestCopyCmdResultSet()
        {
            var stageName = TestName;
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola STRING" });

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"create or replace stage {stageName}";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"copy into {TableName} from @{stageName}";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.AreEqual(true, rdr.Read());
                }

                // test rows_loaded exists
                cmd.CommandText = $"copy into @%{TableName} from (select 'test_string')";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.AreEqual(true, rdr.Read());
                }

                cmd.CommandText = $"copy into {TableName}";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.AreEqual(true, rdr.Read());
                }

                // clean up
                cmd.CommandText = $"drop stage {stageName}";
                cmd.ExecuteNonQuery();

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestRetrieveSemiStructuredData()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                    {
                        "cola VARIANT",
                        "colb ARRAY",
                        "colc OBJECT"
                    },
                    "as select '[\"1\", \"2\"]', '[\"1\", \"2\"]', '{\"key\": \"value\"}'");

                IDbCommand cmd = conn.CreateCommand();

                cmd.CommandText = $"select * from {TableName}";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);

                    Assert.AreEqual(true, reader.Read());
                    Assert.AreEqual("[\n  \"1\",\n  \"2\"\n]", reader.GetString(0));
                    Assert.AreEqual("[\n  \"1\",\n  \"2\"\n]", reader.GetString(1));
                    Assert.AreEqual("{\n  \"key\": \"value\"\n}", reader.GetString(2));
                }

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestResultSetMetadata()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[]
                {
                    "c1 NUMBER(20, 4)",
                    "c2 STRING(100)",
                    "c3 DOUBLE",
                    "c4 TIMESTAMP_NTZ",
                    "c5 VARIANT not null",
                    "c6 BOOLEAN"
                });

                IDbCommand cmd = conn.CreateCommand();

                cmd.CommandText = $"select * from {TableName}";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);

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

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestHasRows()
        {
            using (var conn = CreateAndOpenConnection())
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1 where 1=2";

                DbDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.IsFalse(reader.HasRows);
                reader.Close();
                CloseConnection(conn);
            }
        }

        [Test]
        public void TestHasRowsMultiStatement()
        {
            using (var conn = CreateAndOpenConnection())
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1;" +
                                  "select 1 where 1=2;" +
                                  "select 1;" +
                                  "select 1 where 1=2;";

                DbParameter param = cmd.CreateParameter();
                param.ParameterName = "MULTI_STATEMENT_COUNT";
                param.DbType = DbType.Int16;
                param.Value = 4;
                cmd.Parameters.Add(param);

                DbDataReader reader = cmd.ExecuteReader();

                Assert.AreEqual(_resultFormat, ((SnowflakeDbDataReader)reader).ResultFormat);

                // select 1
                Assert.IsTrue(reader.HasRows);
                reader.Read();
                Assert.IsTrue(reader.HasRows);
                reader.NextResult();

                // select 1 where 1=2
                Assert.IsFalse(reader.HasRows);
                reader.NextResult();

                // select 1
                Assert.IsTrue(reader.HasRows);
                reader.Read();
                Assert.IsTrue(reader.HasRows);
                reader.NextResult();

                // select 1 where 1=2
                Assert.IsFalse(reader.HasRows);
                reader.NextResult();
                Assert.IsFalse(reader.HasRows);

                reader.Close();
                CloseConnection(conn);
            }
        }

        [Test]
        [TestCase("99")]                           // Int8
        [TestCase("9.9")]                          // Int8 + scale
        [TestCase("999")]                          // Int16
        [TestCase("9.99")]                         // Int16 + scale
        [TestCase("99999999")]                     // Int32
        [TestCase("999999.99")]                    // Int32 + scale
        [TestCase("99999999999")]                  // Int64
        [TestCase("999999999.99")]                 // Int64 + scale
        [TestCase("999999999999999999999999999")]  // Decimal
        [TestCase("9999999999999999999999999.99")] // Decimal + scale
        public void TestNumericValues(string testValue)
        {
            using (var conn = CreateAndOpenConnection())
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select " + testValue;
                using (SnowflakeDbDataReader reader = (SnowflakeDbDataReader)cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);

                    while (reader.Read())
                    {
                        Assert.AreEqual(Convert.ToDecimal(testValue), reader.GetDecimal(0));
                        Assert.AreEqual(Convert.ToDouble(testValue), reader.GetDouble(0));
                        Assert.AreEqual(Convert.ToSingle(testValue), reader.GetFloat(0));
                        if (!testValue.Contains('.'))
                        {
                            decimal value = Decimal.Parse(testValue);
                            if (value >= Int64.MinValue && value <= Int64.MaxValue)
                                Assert.AreEqual(Convert.ToInt64(testValue), reader.GetInt64(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetInt64(0));
                            if (value >= Int32.MinValue && value <= Int32.MaxValue)
                                Assert.AreEqual(Convert.ToInt32(testValue), reader.GetInt32(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetInt32(0));
                            if (value >= Int16.MinValue && value <= Int16.MaxValue)
                                Assert.AreEqual(Convert.ToInt16(testValue), reader.GetInt16(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetInt16(0));
                            if (value >= 0 && value <= 255)
                                Assert.AreEqual(Convert.ToByte(testValue), reader.GetByte(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetByte(0));
                        }
                    }
                    CloseConnection(conn);
                }
            }
        }

        [Test]
        [TestCase("2019-01-01 12:12:12.1234567 +0500", 7)]
        [TestCase("2019-01-01 12:12:12.1234567 -0500", 7)]
        [TestCase("2019-01-01 12:12:12.1234567 +1400", 7)]
        [TestCase("2019-01-01 12:12:12.1234567 -1400", 7)]
        [TestCase("0001-01-01 00:00:00.0000000 +0000", 9)]
        [TestCase("9999-12-31 23:59:59.9999999 +0000", 9)]
        public void TestTimestampTz(string testValue, int scale)
        {
            using (var conn = CreateAndOpenConnection())
            {
                DbCommand cmd = conn.CreateCommand();

                cmd.CommandText = $"select '{testValue}'::TIMESTAMP_TZ({scale})";
                using (SnowflakeDbDataReader reader = (SnowflakeDbDataReader)cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);

                    reader.Read();

                    var expectedValue = DateTimeOffset.Parse(testValue);

                    Assert.AreEqual(expectedValue, reader.GetValue(0));
                }

                CloseConnection(conn);
            }
        }

        [Test]
        [TestCase("2019-01-01 12:12:12.1234567 +0200", 7, "2019-01-01 02:12:12.1234567 -08:00")]
        [TestCase("2019-01-01 12:12:12.1234567 +1400", 7, "2018-12-31 14:12:12.1234567 -08:00")]
        [TestCase("0001-01-02 00:00:00.0000000 +0000", 9, null)]
        [TestCase("1883-11-19 00:00:00.0000000 +0000", 9, "1883-11-18 16:00:00.0000000 -08:00")]
        [TestCase("9999-12-31 23:59:59.9999999 +0000", 9, "9999-12-31 15:59:59.9999999 -08:00")]
        [TestCase("2019-01-01 12:12:12.1234567", 7, "2019-01-01 12:12:12.1234567 -08:00")]
        public void TestTimestampLtz(string testValue, int scale, string expectedValue)
        {
            using (var conn = CreateAndOpenConnectionWithHonorSessionTimezone())
            {
                DbCommand cmd = conn.CreateCommand();

                cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"select '{testValue}'::TIMESTAMP_LTZ({scale})";
                using (SnowflakeDbDataReader reader = (SnowflakeDbDataReader)cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);

                    reader.Read();

                    var expected = expectedValue != null
                        ? DateTimeOffset.Parse(expectedValue)
                        : ComputeExpectedLtzOffset(testValue, "America/Los_Angeles");

                    Assert.AreEqual(expected, reader.GetValue(0));
                }

                CloseConnection(conn);
            }
        }

        [Test]
        [TestCase("2019-01-01 12:12:12.1234567", 7)]
        [TestCase("0001-01-01 00:00:00.0000000", 9)]
        [TestCase("9999-12-31 23:59:59.9999999", 9)]
        public void TestTimestampNtz(string testValue, int scale)
        {
            using (var conn = CreateAndOpenConnection())
            {
                DbCommand cmd = conn.CreateCommand();

                cmd.CommandText = $"select '{testValue}'::TIMESTAMP_NTZ({scale})";
                using (SnowflakeDbDataReader reader = (SnowflakeDbDataReader)cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);

                    reader.Read();

                    var expectedValue = DateTime.Parse(testValue);

                    Assert.AreEqual(expectedValue, reader.GetValue(0));
                }

                CloseConnection(conn);
            }
        }

        [Test]
        [TestCase("array")]
        [TestCase("object")]
        [TestCase("variant")]
        public void TestDataTableLoadOnSemiStructuredColumn(string type)
        {
            using (var conn = CreateAndOpenConnection())
            {
                var colName = "c1";
                var expectedVal = "id:1";
                CreateOrReplaceTable(conn, TableName, new[] { $"{colName} {type}" });

                using (var cmd = conn.CreateCommand())
                {
                    string insertCommand = $"insert into {TableName} select parse_json('{{{expectedVal}}}')";
                    cmd.CommandText = insertCommand;

                    var count = cmd.ExecuteNonQuery();
                    Assert.AreEqual(1, count);

                    cmd.CommandText = $"select {colName} from {TableName}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);
                        var dt = new DataTable();
                        dt.Load(reader);
                        Assert.AreEqual(expectedVal, DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

        [Test]
        public void TestTimestampLtzHonorsSessionTimezone()
        {
            using (var conn = CreateAndOpenConnectionWithHonorSessionTimezone())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "val TIMESTAMP_LTZ" });

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"INSERT INTO {TableName} VALUES('2023-08-09 10:00:00')";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"SELECT * FROM {TableName}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read(), "Should read a record");
                        var timestamp1 = reader.GetDateTime(0);

                        var warsawTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Europe/Warsaw");
                        var expectedTime1 = new DateTime(2023, 8, 9, 10, 0, 0, DateTimeKind.Unspecified);
                        var expectedUtc1 = TimeZoneInfo.ConvertTimeToUtc(expectedTime1, warsawTz);
                        var expectedInWarsaw = TimeZoneInfo.ConvertTimeFromUtc(expectedUtc1, warsawTz);

                        Assert.AreEqual(expectedInWarsaw, timestamp1,
                            $"Timestamp should be returned in Warsaw timezone. Expected: {expectedInWarsaw}, Got: {timestamp1}");
                    }

                    cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'Pacific/Honolulu'";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"SELECT * FROM {TableName}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read(), "Should read a record");
                        var timestamp2 = reader.GetDateTime(0);

                        var honoluluTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Pacific/Honolulu");
                        var warsawTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Europe/Warsaw");

                        var originalTimeInWarsaw = new DateTime(2023, 8, 9, 10, 0, 0, DateTimeKind.Unspecified);
                        var utcTime = TimeZoneInfo.ConvertTimeToUtc(originalTimeInWarsaw, warsawTz);
                        var expectedInHonolulu = TimeZoneInfo.ConvertTimeFromUtc(utcTime, honoluluTz);

                        Assert.AreEqual(expectedInHonolulu, timestamp2,
                            $"Timestamp should be returned in Honolulu timezone. Expected: {expectedInHonolulu}, Got: {timestamp2}");
                    }
                }

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestTimestampLtzWithMultipleSessionTimezones()
        {
            using (var conn = CreateAndOpenConnectionWithHonorSessionTimezone())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "val TIMESTAMP_LTZ" });

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'UTC'";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"INSERT INTO {TableName} VALUES('2024-01-01 00:00:00')";
                    cmd.ExecuteNonQuery();

                    var utcBase = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

                    // Test reading with different timezones
                    var timezones = new[]
                    {
                        "Europe/Warsaw",
                        "Asia/Tokyo",
                        "America/Los_Angeles"
                    };

                    foreach (var tzName in timezones)
                    {
                        cmd.CommandText = $"ALTER SESSION SET TIMEZONE = '{tzName}'";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = $"SELECT val FROM {TableName}";
                        using (var reader = cmd.ExecuteReader())
                        {
                            Assert.IsTrue(reader.Read());
                            var timestamp = reader.GetDateTime(0);

                            var tz = TimeZoneConverter.TZConvert.GetTimeZoneInfo(tzName);
                            var expected = TimeZoneInfo.ConvertTimeFromUtc(utcBase, tz);

                            Assert.AreEqual(expected, timestamp,
                                $"TIMESTAMP_LTZ should be in {tzName} timezone");
                        }
                    }
                }

                CloseConnection(conn);
            }
        }

        [Test]
        public void TestTimestampLtzUsesLocalTimezoneByDefault()
        {
            // Verifies that without HonorSessionTimezone, TIMESTAMP_LTZ uses the local machine
            // timezone regardless of what the session timezone is set to.
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "val TIMESTAMP_LTZ" });

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'UTC'";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = $"INSERT INTO {TableName} VALUES('2024-06-15 12:00:00')";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'Pacific/Auckland'";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"SELECT val FROM {TableName}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read());
                        var timestamp = (DateTimeOffset)reader.GetValue(0);

                        var utcTime = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
                        var expectedLocal = TimeZoneInfo.ConvertTimeFromUtc(utcTime, TimeZoneInfo.Local);
                        var expectedOffset = TimeZoneInfo.Local.GetUtcOffset(expectedLocal);

                        Assert.AreEqual(expectedLocal, timestamp.DateTime,
                            "TIMESTAMP_LTZ should be in local machine timezone when HonorSessionTimezone is not set");
                        Assert.AreEqual(expectedOffset, timestamp.Offset,
                            "Offset should match local machine timezone, not session timezone (Auckland)");

                        var aucklandTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Pacific/Auckland");
                        var aucklandOffset = aucklandTz.GetUtcOffset(utcTime);
                        if (aucklandOffset != expectedOffset)
                        {
                            Assert.AreNotEqual(aucklandOffset, timestamp.Offset,
                                "Offset must NOT match Auckland timezone when HonorSessionTimezone is not set");
                        }
                    }
                }

                CloseConnection(conn);
            }
        }

        private DbConnection CreateAndOpenConnection()
        {
            var conn = new SnowflakeDbConnection(ConnectionString);
            conn.Open();
            SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
            return conn;
        }

        private DbConnection CreateAndOpenConnectionWithHonorSessionTimezone()
        {
            var conn = new SnowflakeDbConnection(ConnectionString + "HonorSessionTimezone=true;");
            conn.Open();
            SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
            return conn;
        }

        private static DateTimeOffset ComputeExpectedLtzOffset(string inputValue, string timezoneName)
        {
            var inputDto = DateTimeOffset.Parse(inputValue);
            var tz = TimeZoneConverter.TZConvert.GetTimeZoneInfo(timezoneName);
            var offset = tz.GetUtcOffset(inputDto.UtcDateTime);
            var localDt = inputDto.UtcDateTime + offset;
            return new DateTimeOffset(localDt, offset);
        }

        private void CloseConnection(DbConnection conn)
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
            conn.Close();
        }
    }
}
