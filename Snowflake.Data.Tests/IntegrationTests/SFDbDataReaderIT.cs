using System;
using System.Linq;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Text;
using Xunit;
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
            Assert.Equal(_resultFormat, ((SnowflakeDbDataReader)reader).ResultFormat);
        }

        [SFFact]
        public void TestRecordsAffected()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER" });

                IDbCommand cmd = conn.CreateCommand();

                string insertCommand = $"insert into {TableName} values (1),(1),(1)";
                cmd.CommandText = insertCommand;
                IDataReader reader = cmd.ExecuteReader();
                Assert.Equal(3, reader.RecordsAffected);

                // Reader's RecordsAffected should be available even if the reader is closed
                reader.Close();
                Assert.Equal(3, reader.RecordsAffected);

                cmd.CommandText = $"drop table if exists {TableName}";
                var count = cmd.ExecuteNonQuery();
                Assert.Equal(0, count);

                // Reader's RecordsAffected should be available even if the connection is closed
                CloseConnection(conn);
                Assert.Equal(3, reader.RecordsAffected);
            }
        }

        [SFFact]
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
                Assert.Equal(3, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                Assert.Equal(numInt, reader.GetInt32(0));

                Assert.True(reader.Read());
                Assert.Equal(numLong, reader.GetInt64(0));

                Assert.True(reader.Read());
                Assert.Equal(numShort, reader.GetInt16(0));

                Assert.False(reader.Read());
                reader.Close();

                CloseConnection(conn);
            }

        }

        [SFFact]
        [InlineData("NUMBER(18,10)")]
        [InlineData("NUMBER(18,12)")]
        [InlineData("NUMBER(38,20)")]
        [InlineData("NUMBER(38,28)")]
        public void TestGetNumberWithHighScale(string columnType)
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { $"cola {columnType}" });

                var cmd = conn.CreateCommand();
                cmd.CommandText = $"INSERT INTO {TableName} SELECT 1.23";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"SELECT cola FROM {TableName}";
                using (var reader = cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);
                    Assert.Equal("FIXED", reader.GetDataTypeName(0));
                    Assert.Equal(typeof(decimal), reader.GetFieldType(0));
                    Assert.True(reader.Read());
                    var rawValue = reader.GetValue(0);
                    Assert.InstanceOf<decimal>(rawValue);
                    Assert.Equal(1.23m, (decimal)rawValue);
                    Assert.Equal(1.23m, reader.GetDecimal(0));
                    Assert.False(reader.Read());
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
        public void TestGetDecfloatWithHighPrecision()
        {
            using (var conn = CreateAndOpenConnection())
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 12345678901234567890.123456789::DECFLOAT";
                using (var reader = cmd.ExecuteReader())
                {
                    ValidateResultFormat(reader);
                    Assert.Equal("DECFLOAT", reader.GetDataTypeName(0));
                    Assert.Equal(typeof(string), reader.GetFieldType(0));
                    Assert.True(reader.Read());
                    var value = reader.GetValue(0);
                    Assert.InstanceOf<string>(value);
                    Assert.That((string)value, Does.Contain("1234567890123456789"));
                    Assert.Equal((string)value, reader.GetString(0));
                    Assert.False(reader.Read());
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                Assert.Equal(2, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                Assert.Equal(numFloat, reader.GetFloat(0));
                Assert.Equal((decimal)numFloat, reader.GetDecimal(0));


                Assert.True(reader.Read());
                Assert.Equal(numDouble, reader.GetDouble(0));

                Assert.False(reader.Read());
                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
        [InlineData(null)]
        [InlineData("9999-12-31 00:00:00.0000000")]
        [InlineData("9999-12-30 00:00:00.0000000")]
        [InlineData("1982-01-18 00:00:00.0000000")]
        [InlineData("1969-07-21 00:00:00.0000000")]
        [InlineData("1900-09-03 00:00:00.0000000")]
        public void TestGetDate(string inputTimeStr)
        {
            TestGetDateAndOrTime(inputTimeStr, null, SFDataType.DATE);
        }

        [SFFact]
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

                    Assert.True(reader.Read());
                    Assert.Equal("05/17/2013", reader.GetString(0));

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

        [SFFact]
        [InlineData(null, null)]
        [InlineData(null, 3)]
        [InlineData("9999-12-31 23:59:59.9999999", null)]
        [InlineData("9999-12-31 23:59:59.9999999", 5)]
        [InlineData("1982-01-18 16:20:00.6666666", null)]
        [InlineData("1982-01-18 16:20:00.6666666", 3)]
        [InlineData("1969-07-21 02:56:15.1234567", null)]
        [InlineData("1969-07-21 02:56:15.1234567", 1)]
        [InlineData("1900-09-03 12:12:12.1212121", null)]
        [InlineData("1900-09-03 12:12:12.1212121", 1)]
        public void TestGetTime(string inputTimeStr, int? precision)
        {
            TestGetDateAndOrTime(inputTimeStr, precision, SFDataType.TIME);
        }

        [SFFact]
        [InlineData("11:22:33.4455667")]
        [InlineData("23:59:59.9999999")]
        [InlineData("16:20:00.6666666")]
        [InlineData("00:00:00.0000000")]
        [InlineData("00:00:00")]
        [InlineData("23:59:59.1")]
        [InlineData("23:59:59.12")]
        [InlineData("23:59:59.123")]
        [InlineData("23:59:59.1234")]
        [InlineData("23:59:59.12345")]
        [InlineData("23:59:59.123456")]
        [InlineData("23:59:59.1234567")]
        [InlineData("23:59:59.12345678")]
        [InlineData("23:59:59.123456789")]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"SELECT cola FROM {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());

                // For time, we getDateTime on the column and ignore date part
                DateTime dateTimeTime = reader.GetDateTime(0);
                TimeSpan timeSpanTime = ((SnowflakeDbDataReader)reader).GetTimeSpan(0);
                reader.Close();

                // The expected result. Timespan precision only goes up to 7 digits
                TimeSpan expected = TimeSpan.ParseExact(inputTimeStr.Length < 16 ? inputTimeStr : inputTimeStr.Substring(0, 16), "c", CultureInfo.InvariantCulture);
                // Verify the result
                Assert.Equal(expected, timeSpanTime);
                Assert.Equal(dateTimeTime.Hour, timeSpanTime.Hours);
                Assert.Equal(dateTimeTime.Minute, timeSpanTime.Minutes);
                Assert.Equal(dateTimeTime.Second, timeSpanTime.Seconds);
                Assert.Equal(dateTimeTime.Millisecond, timeSpanTime.Milliseconds);

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                Assert.Equal(1, count);

                insertCommand = $"update {TableName} set C2 = 2.5, C3 = 'C3Val', C4 = TO_BINARY('C4'), C5 = true, C6 = '2021-01-01', " +
                "C7 = '2017-01-01 12:00:00', C8 = '2017-01-01 12:00:00 +04:00', C9 = '2014-01-02 16:00:00 +10:00', C14 = '12:00:00' where C1 = 1";
                cmd.CommandText = insertCommand;
                //Console.WriteLine(insertCommand);
                count = cmd.ExecuteNonQuery();
                Assert.Equal(1, count);

                cmd.CommandText = $"SELECT * FROM {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());

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
                        Assert.Equal(270003, e.ErrorCode);
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());

                // For time, we getDateTime on the column and ignore date part
                DateTime actualTime = reader.GetDateTime(0);

                if (dataType == SFDataType.DATE)
                {
                    Assert.Equal(inputTime.Date, actualTime);
                    Assert.Equal(inputTime.Date.ToString("yyyy-MM-dd"), reader.GetString(0));
                }
                if (dataType != SFDataType.DATE)
                {
                    var inputTimeTicksOfTheDay = inputTime.Ticks - inputTime.Date.Ticks;
                    var actualTimeTicksOfTheDay = actualTime.Ticks - actualTime.Date.Ticks;
                    var allowedPrecisionLossInTicks = precision < 7 ? Math.Pow(10, (double)(7 - precision)) - 1 : 0d;
                    Assert.Equal(inputTimeTicksOfTheDay, actualTimeTicksOfTheDay, allowedPrecisionLossInTicks);
                }
                if (dataType == SFDataType.TIMESTAMP_NTZ)
                {
                    if (precision == 9)
                    {
                        Assert.Equal(inputTime, actualTime);
                    }
                    else
                    {
                        Assert.Equal(inputTime.Date, actualTime.Date);
                    }
                }

                // DATE, TIME and TIMESTAMP_NTZ should be returned with DateTimeKind.Unspecified
                Assert.Equal(DateTimeKind.Unspecified, actualTime.Kind);

                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
        [InlineData(null, null)]
        [InlineData(null, 3)]
        [InlineData("2100-12-31 23:59:59.9999999", null)]
        [InlineData("2100-12-31 23:59:59.9999999", 5)]
        [InlineData("9999-12-31 23:59:59.9999999", null)]
        [InlineData("9999-12-31 23:59:59.9999999", 5)]
        [InlineData("9999-12-30 23:59:59.9999999", null)]
        [InlineData("9999-12-30 23:59:59.9999999", 5)]
        [InlineData("1982-01-18 16:20:00.6666666", null)]
        [InlineData("1982-01-18 16:20:00.6666666", 3)]
        //[InlineData("1969-07-21 02:56:15.1234567", null)] //parsing fails with dates with second fractions before the unix epoch
        [InlineData("1969-07-21 02:56:15.0000000", 1)] //dates w/o second fractions before the unix epoch are fine
        //[InlineData("1900-09-03 12:12:12.1212121", null)] // fails
        [InlineData("1900-09-03 12:12:12.0000000", 1)]
        public void TestGetTimestampNTZ(string inputTimeStr, int? precision)
        {
            TestGetDateAndOrTime(inputTimeStr, precision, SFDataType.TIMESTAMP_NTZ);
        }


        [SFFact]
        [InlineData(0)]
        [InlineData(5)]
        [InlineData(-5)]
        [InlineData(14)]
        [InlineData(-14)]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                DateTimeOffset dtOffset = (DateTimeOffset)reader.GetValue(0);
                reader.Close();

                Assert.Equal(now, dtOffset);
                Assert.Equal(now.Offset, dtOffset.Offset);

                CloseConnection(conn);
            }

        }

        [SFFact]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                DateTimeOffset dtOffset = (DateTimeOffset)reader.GetValue(0);
                reader.Close();

                Assert.Equal(insertValue.UtcDateTime, dtOffset.UtcDateTime);

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                Assert.Equal(value, reader.GetBoolean(0));
                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                        Assert.Equal(testBytes[index++], reader.GetByte(0));
                    }
                }
            }
        }

        [SFFact]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                // Auto type conversion
                Assert.True(testBytes.SequenceEqual((byte[])reader.GetValue(0)));
                Assert.True(testChars.Equals(reader.GetValue(1)));
                Assert.True(testDouble.Equals(reader.GetValue(2)));

                // Read all 'TEST_GET_BINARAY' data
                int toReadLength = testBytes.Length;
                byte[] sub = new byte[toReadLength];
                long read = reader.GetBytes(0, 0, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testBytes.SequenceEqual(sub));

                // Read subset 'GET_BINARAY' from actual 'TEST_GET_BINARAY' data
                toReadLength = 11;
                byte[] testSubBytes = Encoding.UTF8.GetBytes("GET_BINARAY");
                sub = new byte[toReadLength];
                read = reader.GetBytes(0, 5, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubBytes.SequenceEqual(sub));

                // Read subset 'GET_CHARS' from actual 'TEST_GET_CHARS' data
                toReadLength = 9;
                testSubBytes = Encoding.UTF8.GetBytes("GET_CHARS");
                sub = new byte[toReadLength];
                read = reader.GetBytes(1, 5, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubBytes.SequenceEqual(sub));

                // Read subset '5678' from actual '1.2345678' data
                toReadLength = 4;
                testSubBytes = Encoding.UTF8.GetBytes("5678");
                sub = new byte[toReadLength];
                read = reader.GetBytes(2, 5, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubBytes.SequenceEqual(sub));

                // Read subset 'GET_BINARAY'  from actual 'TEST_GET_BINARAY' data
                // and copy inside existing buffer replacing Xs
                toReadLength = 11;
                byte[] testSubBytesWithTargetOffset = Encoding.UTF8.GetBytes("OFFSET GET_BINARAY EXTRA");
                sub = Encoding.UTF8.GetBytes("OFFSET XXXXXXXXXXX EXTRA");
                read = reader.GetBytes(0, 5, sub, 7, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubBytesWithTargetOffset.SequenceEqual(sub));

                // Less data than 'ask' for
                int dataOffset = 10;
                read = reader.GetBytes(0, dataOffset, sub, 0, toReadLength);
                Assert.Equal(read, testBytes.Length - dataOffset);

                //** Invalid data offsets **/
                try
                {
                    // Data offset > data length
                    reader.GetBytes(0, 25, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.Equal("dataOffset", e.ParamName);
                }

                try
                {
                    // Data offset < 0
                    reader.GetBytes(0, -1, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.Equal("dataOffset", e.ParamName);
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
                    Assert.Equal("buffer", e.ParamName);
                }

                try
                {
                    // Buffer offset < 0
                    reader.GetBytes(0, 6, sub, -1, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.Equal("bufferOffset", e.ParamName);
                }

                //** Null buffer **//
                // If null, this method returns the size required of the array in order to fit all
                // of the specified data.
                read = reader.GetBytes(0, 6, null, 0, toReadLength);
                Assert.Equal(testBytes.Length, read);

                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                    Assert.True(reader.Read());
                    Assert.Equal(testChar, reader.GetChar(0));
                }
            }
        }

        [SFFact]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                // Auto type conversion
                Assert.True(testChars.Equals(reader.GetValue(0)));
                Assert.True(testBytes.SequenceEqual((byte[])reader.GetValue(1)));
                Assert.True(testDouble.Equals(reader.GetValue(2)));

                // Read all 'TEST_GET_CHARS' data
                int toReadLength = 14;
                char[] testSubChars = testChars.ToArray<char>();
                char[] sub = new char[toReadLength];
                long read = reader.GetChars(0, 0, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubChars.SequenceEqual(sub));

                // Read subset 'GET_CHARS' from actual 'TEST_GET_CHARS' data
                toReadLength = 9;
                testSubChars = "GET_CHARS".ToArray<char>();
                sub = new char[toReadLength];
                read = reader.GetChars(0, 5, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubChars.SequenceEqual(sub));

                // Read subset 'GET_BINARY' from actual 'TEST_GET_BINARY' data
                toReadLength = 10;
                testSubChars = "GET_BINARY".ToArray<char>();
                sub = new char[toReadLength];
                read = reader.GetChars(1, 5, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubChars.SequenceEqual(sub));

                // Read subset '5678' from actual '1.2345678' data
                toReadLength = 4;
                testSubChars = "5678".ToArray<char>();
                sub = new char[toReadLength];
                read = reader.GetChars(2, 5, sub, 0, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubChars.SequenceEqual(sub));


                // Read subset 'GET_CHARS'  from actual 'TEST_GET_CHARS' data
                // and copy inside existing buffer replacing Xs
                char[] testSubCharsWithTargetOffset = "OFFSET GET_CHARS EXTRA".ToArray<char>();
                toReadLength = 9;
                sub = "OFFSET XXXXXXXXX EXTRA".ToArray<char>();
                read = reader.GetChars(0, 5, sub, 7, toReadLength);
                Assert.Equal(read, toReadLength);
                Assert.True(testSubCharsWithTargetOffset.SequenceEqual(sub));

                // Less data than 'ask' for
                int dataOffset = 10;
                read = reader.GetChars(0, dataOffset, sub, 0, toReadLength);
                Assert.Equal(read, testChars.Length - dataOffset);

                //** Invalid data offsets **//
                try
                {
                    // Data offset > data length
                    reader.GetChars(0, 25, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.Equal("dataOffset", e.ParamName);
                }

                try
                {
                    // Data offset < 0
                    reader.GetChars(0, -1, sub, 7, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.Equal("dataOffset", e.ParamName);
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
                    Assert.Equal("buffer", e.ParamName);
                }

                try
                {
                    // Buffer offset < 0
                    reader.GetChars(0, 6, sub, -1, toReadLength);
                    Assert.Fail();
                }
                catch (ArgumentException e)
                {
                    Assert.Equal("bufferOffset", e.ParamName);
                }

                //** Null buffer **//
                // If null, this method returns the size required of the array in order to fit all
                // of the specified data.
                read = reader.GetChars(0, 6, null, 0, toReadLength);
                Assert.Equal(testChars.Length, read);

                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                    Assert.True(reader.Read());
                    Assert.Equal("TEXT", reader.GetDataTypeName(0));
                    Assert.Equal("BINARY", reader.GetDataTypeName(1));
                    Assert.Equal("REAL", reader.GetDataTypeName(2));
                }
            }
        }

        [SFFact]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                DbDataReader reader = (DbDataReader)cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());

                // Auto type conversion
                Assert.True(testChars.Equals(reader.GetValue(0)));
                Assert.True(testBytes.SequenceEqual((byte[])reader.GetValue(1)));
                Assert.True(testDouble.Equals(reader.GetValue(2)));

                using (var stream = reader.GetStream(0))
                {
                    byte[] col1ToBytes = Encoding.UTF8.GetBytes(testChars);
                    byte[] buf = new byte[col1ToBytes.Length];
                    var readBytes = stream.Read(buf, 0, col1ToBytes.Length);
                    Assert.Equal(col1ToBytes.Length, readBytes);
                    Assert.True(-1 == stream.ReadByte()); // No more data
                    Assert.True(col1ToBytes.SequenceEqual(buf));
                }

                using (var stream = reader.GetStream(1))
                {
                    byte[] buf = new byte[testBytes.Length];
                    var readBytes = stream.Read(buf, 0, testBytes.Length);
                    Assert.Equal(testBytes.Length, readBytes);
                    Assert.True(-1 == stream.ReadByte()); // No more data
                    Assert.True(testBytes.SequenceEqual(buf));
                }

                using (var stream = reader.GetStream(2))
                {
                    byte[] col3ToBytes = Encoding.UTF8.GetBytes(testDouble.ToString());
                    byte[] buf = new byte[col3ToBytes.Length];
                    var readBytes = stream.Read(buf, 0, col3ToBytes.Length);
                    Assert.Equal(col3ToBytes.Length, readBytes);
                    Assert.True(-1 == stream.ReadByte()); // No more data
                    Assert.True(col3ToBytes.SequenceEqual(buf));
                }


                reader.Close();

                CloseConnection(conn);
            }
        }


        [SFFact]
        public void TestGetValueIndexOutOfBound()
        {
            using (var conn = CreateAndOpenConnection())
            {
                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());

                try
                {
                    reader.GetInt16(-1);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270002, e.ErrorCode);
                }

                try
                {
                    reader.GetInt16(1);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.Equal(270002, e.ErrorCode);
                }
                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
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

                        Assert.Equal(2, reader.FieldCount);
                        Assert.Equal(0, reader.Depth);
                        Assert.True(((SnowflakeDbDataReader)reader).HasRows);
                        Assert.False(reader.IsClosed);
                        Assert.Equal("COLONE", reader.GetName(0));
                        Assert.Equal("COLTWO", reader.GetName(1));

                        Assert.Equal(typeof(long), reader.GetFieldType(0));
                        Assert.Equal(typeof(long), reader.GetFieldType(1));

                        Assert.False(reader.NextResult());
                        Assert.Equal(-1, reader.RecordsAffected);

                        Assert.Equal(0, reader.GetOrdinal("COLONE"));
                        // reapet calling to test if cache in memory worked or not
                        Assert.Equal(0, reader.GetOrdinal("COLONE"));
                        Assert.Equal(0, reader.GetOrdinal("COLONE"));
                        Assert.Equal(1, reader.GetOrdinal("COLTWO"));
                        Assert.Equal(-1, reader.GetOrdinal("COL_NOT_EXISTS"));

                        reader.Close();
                        Assert.True(reader.IsClosed);

                        try
                        {
                            reader.Read();
                            Assert.Fail();
                        }
                        catch (SnowflakeDbException e)
                        {
                            Assert.Equal(270010, e.ErrorCode);
                        }

                        try
                        {
                            reader.GetInt16(0);
                            Assert.Fail();
                        }
                        catch (SnowflakeDbException e)
                        {
                            Assert.Equal(270010, e.ErrorCode);
                        }
                    }
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                        Assert.Equal(DBNull.Value, nullVal);
                        Assert.True(reader.IsDBNull(0));
                        Assert.True(reader.IsDBNull(1));

                        reader.Close();
                    }
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                IDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                Assert.Equal(val, reader.GetGuid(0));

                // test using [] operator
                Assert.Equal(val.ToString(), reader[0]);
                Assert.Equal(val.ToString(), reader["COLA"]);

                object[] values = new object[1];
                Assert.Equal(1, reader.GetValues(values));
                Assert.Equal(val.ToString(), values[0]);

                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                Assert.Equal(0, updateCount);

                // test rows_loaded exists
                cmd.CommandText = $"copy into @%{TableName} from (select 'test_string')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = $"copy into {TableName}";
                updateCount = cmd.ExecuteNonQuery();
                Assert.Equal(1, updateCount);

                // clean up
                cmd.CommandText = $"drop stage {stageName}";
                cmd.ExecuteNonQuery();

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                    Assert.Equal(true, rdr.Read());
                }

                // test rows_loaded exists
                cmd.CommandText = $"copy into @%{TableName} from (select 'test_string')";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.Equal(true, rdr.Read());
                }

                cmd.CommandText = $"copy into {TableName}";
                using (var rdr = cmd.ExecuteReader())
                {
                    // Can read the first row
                    Assert.Equal(true, rdr.Read());
                }

                // clean up
                cmd.CommandText = $"drop stage {stageName}";
                cmd.ExecuteNonQuery();

                CloseConnection(conn);
            }
        }

        [SFFact]
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

                    Assert.Equal(true, reader.Read());
                    Assert.Equal("[\n  \"1\",\n  \"2\"\n]", reader.GetString(0));
                    Assert.Equal("[\n  \"1\",\n  \"2\"\n]", reader.GetString(1));
                    Assert.Equal("{\n  \"key\": \"value\"\n}", reader.GetString(2));
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
        public void TestGetVariant()
        {
            using (var conn = CreateAndOpenConnection())
            {
                CreateOrReplaceTable(conn, TableName, new[] { "cola VARIANT" });

                var cmd = conn.CreateCommand();
                var insertCommand = $"insert into {TableName} (cola) select parse_json( (?) )";
                cmd.CommandText = insertCommand;

                var val = "    {\"FieldB\":21,\"FieldA\":37}   ";
                var expectedVal = "{\n  \"FieldA\": 37,\n  \"FieldB\": 21\n}";

                var p1 = new SnowflakeDbParameter("1", SFDataType.TEXT)
                {
                    Value = val
                };
                cmd.Parameters.Add(p1);

                var count = cmd.ExecuteNonQuery();
                Assert.Equal(1, count);

                cmd.CommandText = $"select * from {TableName}";
                var reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.True(reader.Read());
                Assert.Equal(expectedVal, reader.GetString(0));

                reader.Close();

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                    Assert.Equal("C1", row[SchemaTableColumn.ColumnName]);
                    Assert.Equal(0, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.Equal(20, row[SchemaTableColumn.NumericPrecision]);
                    Assert.Equal(4, row[SchemaTableColumn.NumericScale]);
                    Assert.Equal(SFDataType.FIXED, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.Equal(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[1];
                    Assert.Equal("C2", row[SchemaTableColumn.ColumnName]);
                    Assert.Equal(1, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.Equal(100, row[SchemaTableColumn.ColumnSize]);
                    Assert.Equal(SFDataType.TEXT, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.Equal(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[2];
                    Assert.Equal("C3", row[SchemaTableColumn.ColumnName]);
                    Assert.Equal(2, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.Equal(SFDataType.REAL, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.Equal(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[3];
                    Assert.Equal("C4", row[SchemaTableColumn.ColumnName]);
                    Assert.Equal(3, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.Equal(0, row[SchemaTableColumn.NumericPrecision]);
                    Assert.Equal(9, row[SchemaTableColumn.NumericScale]);
                    Assert.Equal(SFDataType.TIMESTAMP_NTZ, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.Equal(true, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[4];
                    Assert.Equal("C5", row[SchemaTableColumn.ColumnName]);
                    Assert.Equal(4, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.Equal(SFDataType.VARIANT, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.Equal(false, row[SchemaTableColumn.AllowDBNull]);

                    row = dataTable.Rows[5];
                    Assert.Equal("C6", row[SchemaTableColumn.ColumnName]);
                    Assert.Equal(5, row[SchemaTableColumn.ColumnOrdinal]);
                    Assert.Equal(SFDataType.BOOLEAN, (SFDataType)row[SchemaTableColumn.ProviderType]);
                    Assert.Equal(true, row[SchemaTableColumn.AllowDBNull]);
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
        public void TestHasRows()
        {
            using (var conn = CreateAndOpenConnection())
            {
                DbCommand cmd = conn.CreateCommand();
                cmd.CommandText = "select 1 where 1=2";

                DbDataReader reader = cmd.ExecuteReader();

                ValidateResultFormat(reader);

                Assert.False(reader.HasRows);
                reader.Close();
                CloseConnection(conn);
            }
        }

        [SFFact]
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

                Assert.Equal(_resultFormat, ((SnowflakeDbDataReader)reader).ResultFormat);

                // select 1
                Assert.True(reader.HasRows);
                reader.Read();
                Assert.True(reader.HasRows);
                reader.NextResult();

                // select 1 where 1=2
                Assert.False(reader.HasRows);
                reader.NextResult();

                // select 1
                Assert.True(reader.HasRows);
                reader.Read();
                Assert.True(reader.HasRows);
                reader.NextResult();

                // select 1 where 1=2
                Assert.False(reader.HasRows);
                reader.NextResult();
                Assert.False(reader.HasRows);

                reader.Close();
                CloseConnection(conn);
            }
        }

        [SFFact]
        [InlineData("99")]                           // Int8
        [InlineData("9.9")]                          // Int8 + scale
        [InlineData("999")]                          // Int16
        [InlineData("9.99")]                         // Int16 + scale
        [InlineData("99999999")]                     // Int32
        [InlineData("999999.99")]                    // Int32 + scale
        [InlineData("99999999999")]                  // Int64
        [InlineData("999999999.99")]                 // Int64 + scale
        [InlineData("999999999999999999999999999")]  // Decimal
        [InlineData("9999999999999999999999999.99")] // Decimal + scale
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
                        Assert.Equal(Convert.ToDecimal(testValue), reader.GetDecimal(0));
                        Assert.Equal(Convert.ToDouble(testValue), reader.GetDouble(0));
                        Assert.Equal(Convert.ToSingle(testValue), reader.GetFloat(0));
                        if (!testValue.Contains('.'))
                        {
                            decimal value = Decimal.Parse(testValue);
                            if (value >= Int64.MinValue && value <= Int64.MaxValue)
                                Assert.Equal(Convert.ToInt64(testValue), reader.GetInt64(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetInt64(0));
                            if (value >= Int32.MinValue && value <= Int32.MaxValue)
                                Assert.Equal(Convert.ToInt32(testValue), reader.GetInt32(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetInt32(0));
                            if (value >= Int16.MinValue && value <= Int16.MaxValue)
                                Assert.Equal(Convert.ToInt16(testValue), reader.GetInt16(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetInt16(0));
                            if (value >= 0 && value <= 255)
                                Assert.Equal(Convert.ToByte(testValue), reader.GetByte(0));
                            else
                                Assert.Throws<OverflowException>(() => reader.GetByte(0));
                        }
                    }
                    CloseConnection(conn);
                }
            }
        }

        [SFFact]
        [InlineData("2019-01-01 12:12:12.1234567 +0500", 7)]
        [InlineData("2019-01-01 12:12:12.1234567 -0500", 7)]
        [InlineData("2019-01-01 12:12:12.1234567 +1400", 7)]
        [InlineData("2019-01-01 12:12:12.1234567 -1400", 7)]
        [InlineData("0001-01-01 00:00:00.0000000 +0000", 9)]
        [InlineData("9999-12-31 23:59:59.9999999 +0000", 9)]
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

                    Assert.Equal(expectedValue, reader.GetValue(0));
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
        [InlineData("2019-01-01 12:12:12.1234567 +0200", 7, "2019-01-01 02:12:12.1234567 -08:00")]
        [InlineData("2019-01-01 12:12:12.1234567 +1400", 7, "2018-12-31 14:12:12.1234567 -08:00")]
        [InlineData("1900-01-15 00:00:00.0000000 +0000", 9, "1900-01-14 16:00:00.0000000 -08:00")]
        [InlineData("1883-11-19 00:00:00.0000000 +0000", 9, "1883-11-18 16:00:00.0000000 -08:00")]
        [InlineData("9999-12-31 23:59:59.9999999 +0000", 9, "9999-12-31 15:59:59.9999999 -08:00")]
        [InlineData("2019-01-01 12:12:12.1234567", 7, "2019-01-01 12:12:12.1234567 -08:00")]
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

                    var expected = DateTimeOffset.Parse(expectedValue);

                    Assert.Equal(expected, reader.GetValue(0));
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
        [InlineData("2019-01-01 12:12:12.1234567", 7)]
        [InlineData("0001-01-01 00:00:00.0000000", 9)]
        [InlineData("9999-12-31 23:59:59.9999999", 9)]
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

                    Assert.Equal(expectedValue, reader.GetValue(0));
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
        [InlineData("array")]
        [InlineData("object")]
        [InlineData("variant")]
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
                    Assert.Equal(1, count);

                    cmd.CommandText = $"select {colName} from {TableName}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        ValidateResultFormat(reader);
                        var dt = new DataTable();
                        dt.Load(reader);
                        Assert.Equal(expectedVal, DataTableParser.GetFirstRowValue(dt, colName));
                    }
                }
            }
        }

        [SFFact]
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
                        Assert.True(reader.Read(), "Should read a record");
                        var timestamp1 = reader.GetDateTime(0);

                        var warsawTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Europe/Warsaw");
                        var expectedTime1 = new DateTime(2023, 8, 9, 10, 0, 0, DateTimeKind.Unspecified);
                        var expectedUtc1 = TimeZoneInfo.ConvertTimeToUtc(expectedTime1, warsawTz);
                        var expectedInWarsaw = TimeZoneInfo.ConvertTimeFromUtc(expectedUtc1, warsawTz);

                        Assert.Equal(expectedInWarsaw, timestamp1,
                            $"Timestamp should be returned in Warsaw timezone. Expected: {expectedInWarsaw}, Got: {timestamp1}");
                    }

                    cmd.CommandText = "ALTER SESSION SET TIMEZONE = 'Pacific/Honolulu'";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = $"SELECT * FROM {TableName}";
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.True(reader.Read(), "Should read a record");
                        var timestamp2 = reader.GetDateTime(0);

                        var honoluluTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Pacific/Honolulu");
                        var warsawTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Europe/Warsaw");

                        var originalTimeInWarsaw = new DateTime(2023, 8, 9, 10, 0, 0, DateTimeKind.Unspecified);
                        var utcTime = TimeZoneInfo.ConvertTimeToUtc(originalTimeInWarsaw, warsawTz);
                        var expectedInHonolulu = TimeZoneInfo.ConvertTimeFromUtc(utcTime, honoluluTz);

                        Assert.Equal(expectedInHonolulu, timestamp2,
                            $"Timestamp should be returned in Honolulu timezone. Expected: {expectedInHonolulu}, Got: {timestamp2}");
                    }
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                            Assert.True(reader.Read());
                            var timestamp = reader.GetDateTime(0);

                            var tz = TimeZoneConverter.TZConvert.GetTimeZoneInfo(tzName);
                            var expected = TimeZoneInfo.ConvertTimeFromUtc(utcBase, tz);

                            Assert.Equal(expected, timestamp,
                                $"TIMESTAMP_LTZ should be in {tzName} timezone");
                        }
                    }
                }

                CloseConnection(conn);
            }
        }

        [SFFact]
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
                        Assert.True(reader.Read());
                        var timestamp = (DateTimeOffset)reader.GetValue(0);

                        var utcTime = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
                        var expectedLocal = TimeZoneInfo.ConvertTimeFromUtc(utcTime, TimeZoneInfo.Local);
                        var expectedOffset = TimeZoneInfo.Local.GetUtcOffset(expectedLocal);

                        Assert.Equal(expectedLocal, timestamp.DateTime,
                            "TIMESTAMP_LTZ should be in local machine timezone when HonorSessionTimezone is not set");
                        Assert.Equal(expectedOffset, timestamp.Offset,
                            "Offset should match local machine timezone, not session timezone (Auckland)");

                        var aucklandTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Pacific/Auckland");
                        var aucklandOffset = aucklandTz.GetUtcOffset(utcTime);
                        if (aucklandOffset != expectedOffset)
                        {
                            Assert.NotEqual(aucklandOffset, timestamp.Offset,
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

        private void CloseConnection(DbConnection conn)
        {
            SessionParameterAlterer.RestoreResultFormat(conn);
            conn.Close();
        }
    }
}
