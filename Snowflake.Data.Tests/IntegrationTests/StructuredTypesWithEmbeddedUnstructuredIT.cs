using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class StructuredTypesWithEmbeddedUnstructuredIT : StructuredTypesIT
    {
        public StructuredTypesWithEmbeddedUnstructuredIT(TestEnvironmentFixture envFixture) : base(envFixture) { }

        // Connection string with HonorSessionTimezone enabled for tests that use session timezone
        private string ConnectionStringWithHonorSessionTimezone => ConnectionString + "HonorSessionTimezone=true;";

        [Fact]
        public void TestSelectAllUnstructuredTypesObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var timeZone = GetTimeZone(connection);
                    var expectedOffset = timeZone.GetUtcOffset(DateTime.Parse("2024-07-11 14:20:05"));
                    var expectedOffsetString = ToOffsetString(expectedOffset);
                    var allTypesObjectAsSFString = @"OBJECT_CONSTRUCT(
                        'StringValue', 'abcąęśźń',
                        'CharValue', 'x',
                        'ByteValue', 15,
                        'SByteValue', -14,
                        'ShortValue', 1200,
                        'UShortValue', 65000,
                        'IntValue', 150150,
                        'UIntValue', 151151,
                        'LongValue', 9111222333444555666,
                        'ULongValue', 9111222333444555666,
                        'FloatValue', 1.23,
                        'DoubleValue', 1.23,
                        'DecimalValue', 1.23,
                        'BooleanValue', true,
                        'GuidValue', '57af59a1-f010-450a-8c37-8fdc78e6ee93',
                        'DateTimeValue', '2024-07-11 14:20:05'::TIMESTAMP_NTZ,
                        'DateTimeOffsetValue', '2024-07-11 14:20:05'::TIMESTAMP_LTZ,
                        'TimeSpanValue', '14:20:05'::TIME,
                        'BinaryValue', TO_BINARY('this is binary data', 'UTF-8'),
                        'SemiStructuredValue', OBJECT_CONSTRUCT('a', 'b')
                    )::OBJECT(
                        StringValue VARCHAR,
                        CharValue CHAR,
                        ByteValue SMALLINT,
                        SByteValue SMALLINT,
                        ShortValue SMALLINT,
                        UShortValue INTEGER,
                        IntValue INTEGER,
                        UIntValue INTEGER,
                        LongValue BIGINT,
                        ULongValue BIGINT,
                        FloatValue FLOAT,
                        DoubleValue DOUBLE,
                        DecimalValue REAL,
                        BooleanValue BOOLEAN,
                        GuidValue TEXT,
                        DateTimeValue TIMESTAMP_NTZ,
                        DateTimeOffsetValue TIMESTAMP_LTZ,
                        TimeSpanValue TIME,
                        BinaryValue BINARY,
                        SemiStructuredValue OBJECT
                    ), '2024-07-11 14:20:05'::TIMESTAMP_LTZ";
                    var bytesForBinary = Encoding.UTF8.GetBytes("this is binary data");
                    command.CommandText = $"SELECT {allTypesObjectAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var allUnstructuredTypesObject = reader.GetObject<AllUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.Equal("abcąęśźń", allUnstructuredTypesObject.StringValue);
                    Assert.Equal('x', allUnstructuredTypesObject.CharValue);
                    Assert.Equal(15, allUnstructuredTypesObject.ByteValue);
                    Assert.Equal(-14, allUnstructuredTypesObject.SByteValue);
                    Assert.Equal(1200, allUnstructuredTypesObject.ShortValue);
                    Assert.Equal(65000, allUnstructuredTypesObject.UShortValue);
                    Assert.Equal(150150, allUnstructuredTypesObject.IntValue);
                    Assert.Equal(151151, allUnstructuredTypesObject.UIntValue);
                    Assert.Equal(9111222333444555666, allUnstructuredTypesObject.LongValue);
                    Assert.Equal(9111222333444555666, allUnstructuredTypesObject.ULongValue);
                    Assert.Equal(1.23f, allUnstructuredTypesObject.FloatValue);
                    Assert.Equal(1.23d, allUnstructuredTypesObject.DoubleValue);
                    Assert.Equal(1.23m, allUnstructuredTypesObject.DecimalValue);
                    Assert.Equal(true, allUnstructuredTypesObject.BooleanValue);
                    Assert.Equal(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allUnstructuredTypesObject.GuidValue);
                    Assert.Equal(DateTime.Parse("2024-07-11 14:20:05"), allUnstructuredTypesObject.DateTimeValue);
                    Assert.Equal(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.Equal(TimeSpan.Parse("14:20:05"), allUnstructuredTypesObject.TimeSpanValue);
                    Assert.Equal(bytesForBinary, allUnstructuredTypesObject.BinaryValue);
                    Assert.Equal(RemoveWhiteSpaces("{\"a\": \"b\"}"), RemoveWhiteSpaces(allUnstructuredTypesObject.SemiStructuredValue));
                }
            }
        }

        [Fact]
        public void TestSelectAllUnstructuredTypesObjectIntoNullableFields()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var timeZone = GetTimeZone(connection);
                    var expectedOffset = timeZone.GetUtcOffset(DateTime.Parse("2024-07-11 14:20:05"));
                    var expectedOffsetString = ToOffsetString(expectedOffset);
                    var allTypesObjectAsSFString = @"OBJECT_CONSTRUCT(
                        'StringValue', 'abc',
                        'CharValue', 'x',
                        'ByteValue', 15,
                        'SByteValue', -14,
                        'ShortValue', 1200,
                        'UShortValue', 65000,
                        'IntValue', 150150,
                        'UIntValue', 151151,
                        'LongValue', 9111222333444555666,
                        'ULongValue', 9111222333444555666,
                        'FloatValue', 1.23,
                        'DoubleValue', 1.23,
                        'DecimalValue', 1.23,
                        'BooleanValue', true,
                        'GuidValue', '57af59a1-f010-450a-8c37-8fdc78e6ee93',
                        'DateTimeValue', '2024-07-11 14:20:05'::TIMESTAMP_NTZ,
                        'DateTimeOffsetValue', '2024-07-11 14:20:05'::TIMESTAMP_LTZ,
                        'TimeSpanValue', '14:20:05'::TIME,
                        'BinaryValue', TO_BINARY('this is binary data', 'UTF-8'),
                        'SemiStructuredValue', OBJECT_CONSTRUCT('a', 'b')
                    )::OBJECT(
                        StringValue VARCHAR,
                        CharValue CHAR,
                        ByteValue SMALLINT,
                        SByteValue SMALLINT,
                        ShortValue SMALLINT,
                        UShortValue INTEGER,
                        IntValue INTEGER,
                        UIntValue INTEGER,
                        LongValue BIGINT,
                        ULongValue BIGINT,
                        FloatValue FLOAT,
                        DoubleValue DOUBLE,
                        DecimalValue REAL,
                        BooleanValue BOOLEAN,
                        GuidValue TEXT,
                        DateTimeValue TIMESTAMP_NTZ,
                        DateTimeOffsetValue TIMESTAMP_LTZ,
                        TimeSpanValue TIME,
                        BinaryValue BINARY,
                        SemiStructuredValue OBJECT
                    )";
                    var bytesForBinary = Encoding.UTF8.GetBytes("this is binary data");
                    command.CommandText = $"SELECT {allTypesObjectAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var allUnstructuredTypesObject = reader.GetObject<AllUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.Equal("abc", allUnstructuredTypesObject.StringValue);
                    Assert.Equal('x', allUnstructuredTypesObject.CharValue);
                    Assert.Equal(15, allUnstructuredTypesObject.ByteValue);
                    Assert.Equal(-14, allUnstructuredTypesObject.SByteValue);
                    Assert.Equal(1200, allUnstructuredTypesObject.ShortValue);
                    Assert.Equal(65000, allUnstructuredTypesObject.UShortValue);
                    Assert.Equal(150150, allUnstructuredTypesObject.IntValue);
                    Assert.Equal(151151, allUnstructuredTypesObject.UIntValue);
                    Assert.Equal(9111222333444555666, allUnstructuredTypesObject.LongValue);
                    Assert.Equal(9111222333444555666, allUnstructuredTypesObject.ULongValue); // there is a problem with 18111222333444555666 value
                    Assert.Equal(1.23f, allUnstructuredTypesObject.FloatValue);
                    Assert.Equal(1.23d, allUnstructuredTypesObject.DoubleValue);
                    Assert.Equal(1.23m, allUnstructuredTypesObject.DecimalValue);
                    Assert.Equal(true, allUnstructuredTypesObject.BooleanValue);
                    Assert.Equal(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allUnstructuredTypesObject.GuidValue);
                    Assert.Equal(DateTime.Parse("2024-07-11 14:20:05"), allUnstructuredTypesObject.DateTimeValue);
                    Assert.Equal(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.Equal(TimeSpan.Parse("14:20:05"), allUnstructuredTypesObject.TimeSpanValue);
                    Assert.Equal(bytesForBinary, allUnstructuredTypesObject.BinaryValue);
                    Assert.Equal(RemoveWhiteSpaces("{\"a\": \"b\"}"), RemoveWhiteSpaces(allUnstructuredTypesObject.SemiStructuredValue));
                }
            }
        }

        [Fact]
        public void TestSelectNullIntoUnstructuredTypesObject()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    var allTypesObjectAsSFString = @"OBJECT_CONSTRUCT_KEEP_NULL(
                        'StringValue', NULL,
                        'CharValue', NULL,
                        'ByteValue', NULL,
                        'SByteValue', NULL,
                        'ShortValue', NULL,
                        'UShortValue', NULL,
                        'IntValue', NULL,
                        'UIntValue', NULL,
                        'LongValue', NULL,
                        'ULongValue', NULL,
                        'FloatValue', NULL,
                        'DoubleValue', NULL,
                        'DecimalValue', NULL,
                        'BooleanValue', NULL,
                        'GuidValue', NULL,
                        'DateTimeValue', NULL,
                        'DateTimeOffsetValue', NULL,
                        'TimeSpanValue', NULL,
                        'BinaryValue', NULL,
                        'SemiStructuredValue', NULL
                    )::OBJECT(
                        StringValue VARCHAR,
                        CharValue CHAR,
                        ByteValue SMALLINT,
                        SByteValue SMALLINT,
                        ShortValue SMALLINT,
                        UShortValue INTEGER,
                        IntValue INTEGER,
                        UIntValue INTEGER,
                        LongValue BIGINT,
                        ULongValue BIGINT,
                        FloatValue FLOAT,
                        DoubleValue DOUBLE,
                        DecimalValue REAL,
                        BooleanValue BOOLEAN,
                        GuidValue TEXT,
                        DateTimeValue TIMESTAMP_NTZ,
                        DateTimeOffsetValue TIMESTAMP_LTZ,
                        TimeSpanValue TIME,
                        BinaryValue BINARY,
                        SemiStructuredValue OBJECT
                    )";
                    command.CommandText = $"SELECT {allTypesObjectAsSFString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act
                    var allUnstructuredTypesObject = reader.GetObject<AllNullableUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.Null(allUnstructuredTypesObject.StringValue);
                    Assert.Null(allUnstructuredTypesObject.CharValue);
                    Assert.Null(allUnstructuredTypesObject.ByteValue);
                    Assert.Null(allUnstructuredTypesObject.SByteValue);
                    Assert.Null(allUnstructuredTypesObject.ShortValue);
                    Assert.Null(allUnstructuredTypesObject.UShortValue);
                    Assert.Null(allUnstructuredTypesObject.IntValue);
                    Assert.Null(allUnstructuredTypesObject.UIntValue);
                    Assert.Null(allUnstructuredTypesObject.LongValue);
                    Assert.Null(allUnstructuredTypesObject.ULongValue);
                    Assert.Null(allUnstructuredTypesObject.FloatValue);
                    Assert.Null(allUnstructuredTypesObject.DoubleValue);
                    Assert.Null(allUnstructuredTypesObject.DecimalValue);
                    Assert.Null(allUnstructuredTypesObject.BooleanValue);
                    Assert.Null(allUnstructuredTypesObject.GuidValue);
                    Assert.Null(allUnstructuredTypesObject.DateTimeValue);
                    Assert.Null(allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.Null(allUnstructuredTypesObject.TimeSpanValue);
                    Assert.Null(allUnstructuredTypesObject.BinaryValue);
                    Assert.Null(allUnstructuredTypesObject.SemiStructuredValue);
                }
            }
        }

        [Theory]
        [MemberData(nameof(DateTimeConversionCases))]
        public void TestSelectDateTime(string dbValue, string dbType, DateTime? expectedRaw, DateTime expected)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionStringWithHonorSessionTimezone))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'";
                    command.ExecuteNonQuery();

                    EnableStructuredTypes(connection);
                    SetTimePrecision(connection, 9);
                    var rawValueString = $"'{dbValue}'::{dbType}";
                    var objectValueString = $"OBJECT_CONSTRUCT('Value', {rawValueString})::OBJECT(Value {dbType})";
                    command.CommandText = $"SELECT {rawValueString}, {objectValueString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act/assert
                    if (expectedRaw != null)
                    {
                        var rawValue = reader.GetDateTime(0);
                        Assert.Equal(expectedRaw, rawValue);
                        Assert.Equal(expectedRaw?.Kind, rawValue.Kind);
                    }
                    var wrappedValue = reader.GetObject<DateTimeWrapper>(1);
                    Assert.Equal(expected, wrappedValue.Value);
                    Assert.Equal(expected.Kind, wrappedValue.Value.Kind);
                }
            }
        }

        internal static IEnumerable<object[]> DateTimeConversionCases()
        {
            yield return new object[]
            {
                "2024-07-11 14:20:05",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("2024-07-11 14:20:05"),
                DateTime.Parse("2024-07-11 14:20:05") // kind -> Unspecified
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05 +5:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11 09:20:05"), DateTimeKind.Utc)
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11 14:20:05"), DateTimeKind.Local)
            };
            yield return new object[]
            {
                "2024-07-11",
                SFDataType.DATE.ToString(),
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11"), DateTimeKind.Unspecified),
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11"), DateTimeKind.Unspecified)
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456789",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("2024-07-11 14:20:05.1234567"),
                DateTime.Parse("2024-07-11 14:20:05.1234568")
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456789 +5:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11 09:20:05.1234568"), DateTimeKind.Utc)
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456789 -2:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11 09:20:05.1234568"), DateTimeKind.Local)
            };
            yield return new object[]
            {
                "9999-12-31 23:59:59.999999",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("9999-12-31 23:59:59.999999"),
                DateTime.Parse("9999-12-31 23:59:59.999999")
            };
            yield return new object[]
            {
                "9999-12-31 23:59:59.999999 +1:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("9999-12-31 22:59:59.999999"), DateTimeKind.Utc)
            };
            yield return new object[]
            {
                "1883-11-19 00:00:00.000000 -5:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                // Expected value in America/Los_Angeles: 1883-11-19 05:00:00 UTC - 8 hours = 1883-11-18 21:00:00 PST
                DateTime.SpecifyKind(DateTime.Parse("1883-11-18 21:00:00.000000"), DateTimeKind.Local)
            };
            yield return new object[]
            {
                "1900-01-15 00:00:00.000000 +0:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("1900-01-14 16:00:00.000000"), DateTimeKind.Local)
            };
            yield return new object[]
            {
                "0001-01-01 00:00:00",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("0001-01-01 00:00:00"),
                DateTime.Parse("0001-01-01 00:00:00")
            };
            yield return new object[]
            {
                "0001-01-01 00:00:00 -1:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTime.SpecifyKind(DateTime.Parse("0001-01-01 01:00:00"), DateTimeKind.Utc)
            };
        }

        [Theory]
        [MemberData(nameof(DateTimeOffsetConversionCases))]
        public void TestSelectDateTimeOffset(string dbValue, string dbType, DateTime? expectedRaw, DateTimeOffset expected)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionStringWithHonorSessionTimezone))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'";
                    command.ExecuteNonQuery();

                    EnableStructuredTypes(connection);
                    SetTimePrecision(connection, 9);
                    var rawValueString = $"'{dbValue}'::{dbType}";
                    var objectValueString = $"OBJECT_CONSTRUCT('Value', {rawValueString})::OBJECT(Value {dbType})";
                    command.CommandText = $"SELECT {rawValueString}, {objectValueString}";
                    var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                    Assert.True(reader.Read());

                    // act/assert
                    if (expectedRaw != null)
                    {
                        var rawValue = reader.GetDateTime(0);
                        Assert.Equal(expectedRaw, rawValue);
                        Assert.Equal(expectedRaw?.Kind, rawValue.Kind);
                    }
                    var wrappedValue = reader.GetObject<DateTimeOffsetWrapper>(1);
                    Assert.Equal(expected, wrappedValue.Value);
                }
            }
        }

        internal static IEnumerable<object[]> DateTimeOffsetConversionCases()
        {
            yield return new object[]
            {
                "2024-07-11 14:20:05",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("2024-07-11 14:20:05"),
                DateTimeOffset.Parse("2024-07-11 14:20:05Z")
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05 +5:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTimeOffset.Parse("2024-07-11 14:20:05 +5:00")
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05 -7:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTimeOffset.Parse("2024-07-11 14:20:05 -7:00")
            };
            yield return new object[]
            {
                "2024-07-11",
                SFDataType.DATE.ToString(),
                DateTime.SpecifyKind(DateTime.Parse("2024-07-11"), DateTimeKind.Unspecified),
                DateTimeOffset.Parse("2024-07-11Z")
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456789",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("2024-07-11 14:20:05.1234567"),
                DateTimeOffset.Parse("2024-07-11 14:20:05.1234568Z")
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456789 +5:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTimeOffset.Parse("2024-07-11 14:20:05.1234568 +5:00")
            };
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456789 -2:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTimeOffset.Parse("2024-07-11 09:20:05.1234568 -7:00")
            };
            yield return new object[]
            {
                "9999-12-31 23:59:59.999999",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("9999-12-31 23:59:59.999999"),
                DateTimeOffset.Parse("9999-12-31 23:59:59.999999Z")
            };
            yield return new object[]
            {
                "9999-12-31 23:59:59.999999 +1:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTimeOffset.Parse("9999-12-31 23:59:59.999999 +1:00")
            };
            yield return new object[]
            {
                "9999-12-31 23:59:59.999999 +13:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTimeOffset.Parse("9999-12-31 02:59:59.999999 -08:00")
            };
            yield return new object[]
            {
                "1900-01-15 00:00:00.000000 +0:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                null,
                DateTimeOffset.Parse("1900-01-14 16:00:00.000000 -08:00")
            };
            yield return new object[]
            {
                "0001-01-01 00:00:00",
                SFDataType.TIMESTAMP_NTZ.ToString(),
                DateTime.Parse("0001-01-01 00:00:00"),
                DateTimeOffset.Parse("0001-01-01 00:00:00Z")
            };
            yield return new object[]
            {
                "0001-01-01 00:00:00 -1:00",
                SFDataType.TIMESTAMP_TZ.ToString(),
                null,
                DateTimeOffset.Parse("0001-01-01 00:00:00 -1:00")
            };
        }

        private TimeZoneInfo GetTimeZone(SnowflakeDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "show parameters like 'timezone'";
                var reader = (SnowflakeDbDataReader)command.ExecuteReader();
                Assert.True(reader.Read());
                var timeZoneString = reader.GetString(1);
                return TimeZoneInfoConverter.FindSystemTimeZoneById(timeZoneString);
            }
        }

        private string ToOffsetString(TimeSpan timeSpan)
        {
            var offsetString = timeSpan.ToString();
            var secondsIndex = offsetString.LastIndexOf(":");
            var offsetWithoutSeconds = offsetString.Substring(0, secondsIndex);
            return offsetWithoutSeconds.StartsWith("+") || offsetWithoutSeconds.StartsWith("-")
                ? offsetWithoutSeconds
                : "+" + offsetWithoutSeconds;
        }

        private void SetTimePrecision(SnowflakeDbConnection connection, int precision)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF{precision}'";
                command.ExecuteNonQuery();
                command.CommandText = $"ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF{precision} TZHTZM'";
                command.ExecuteNonQuery();
            }
        }
    }
}
