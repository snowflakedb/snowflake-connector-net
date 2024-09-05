using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class StructuredTypesWithEmbeddedUnstructuredIT: StructuredTypesIT
    {
        [Test]
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
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act
                    var allUnstructuredTypesObject = reader.GetObject<AllUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.AreEqual("abcąęśźń", allUnstructuredTypesObject.StringValue);
                    Assert.AreEqual('x', allUnstructuredTypesObject.CharValue);
                    Assert.AreEqual(15, allUnstructuredTypesObject.ByteValue);
                    Assert.AreEqual(-14, allUnstructuredTypesObject.SByteValue);
                    Assert.AreEqual(1200, allUnstructuredTypesObject.ShortValue);
                    Assert.AreEqual(65000, allUnstructuredTypesObject.UShortValue);
                    Assert.AreEqual(150150, allUnstructuredTypesObject.IntValue);
                    Assert.AreEqual(151151, allUnstructuredTypesObject.UIntValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.LongValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.ULongValue);
                    Assert.AreEqual(1.23f, allUnstructuredTypesObject.FloatValue);
                    Assert.AreEqual(1.23d, allUnstructuredTypesObject.DoubleValue);
                    Assert.AreEqual(1.23, allUnstructuredTypesObject.DecimalValue);
                    Assert.AreEqual(true, allUnstructuredTypesObject.BooleanValue);
                    Assert.AreEqual(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allUnstructuredTypesObject.GuidValue);
                    Assert.AreEqual(DateTime.Parse("2024-07-11 14:20:05"), allUnstructuredTypesObject.DateTimeValue);
                    Assert.AreEqual(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.AreEqual(TimeSpan.Parse("14:20:05"), allUnstructuredTypesObject.TimeSpanValue);
                    CollectionAssert.AreEqual(bytesForBinary, allUnstructuredTypesObject.BinaryValue);
                    Assert.AreEqual(RemoveWhiteSpaces("{\"a\": \"b\"}"), RemoveWhiteSpaces(allUnstructuredTypesObject.SemiStructuredValue));
                }
            }
        }

        [Test]
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
                    Assert.IsTrue(reader.Read());

                    // act
                    var allUnstructuredTypesObject = reader.GetObject<AllUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.AreEqual("abc", allUnstructuredTypesObject.StringValue);
                    Assert.AreEqual('x', allUnstructuredTypesObject.CharValue);
                    Assert.AreEqual(15, allUnstructuredTypesObject.ByteValue);
                    Assert.AreEqual(-14, allUnstructuredTypesObject.SByteValue);
                    Assert.AreEqual(1200, allUnstructuredTypesObject.ShortValue);
                    Assert.AreEqual(65000, allUnstructuredTypesObject.UShortValue);
                    Assert.AreEqual(150150, allUnstructuredTypesObject.IntValue);
                    Assert.AreEqual(151151, allUnstructuredTypesObject.UIntValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.LongValue);
                    Assert.AreEqual(9111222333444555666, allUnstructuredTypesObject.ULongValue); // there is a problem with 18111222333444555666 value
                    Assert.AreEqual(1.23f, allUnstructuredTypesObject.FloatValue);
                    Assert.AreEqual(1.23d, allUnstructuredTypesObject.DoubleValue);
                    Assert.AreEqual(1.23, allUnstructuredTypesObject.DecimalValue);
                    Assert.AreEqual(true, allUnstructuredTypesObject.BooleanValue);
                    Assert.AreEqual(Guid.Parse("57af59a1-f010-450a-8c37-8fdc78e6ee93"), allUnstructuredTypesObject.GuidValue);
                    Assert.AreEqual(DateTime.Parse("2024-07-11 14:20:05"), allUnstructuredTypesObject.DateTimeValue);
                    Assert.AreEqual(DateTimeOffset.Parse($"2024-07-11 14:20:05 {expectedOffsetString}"), allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.AreEqual(TimeSpan.Parse("14:20:05"), allUnstructuredTypesObject.TimeSpanValue);
                    CollectionAssert.AreEqual(bytesForBinary, allUnstructuredTypesObject.BinaryValue);
                    Assert.AreEqual(RemoveWhiteSpaces("{\"a\": \"b\"}"), RemoveWhiteSpaces(allUnstructuredTypesObject.SemiStructuredValue));
                }
            }
        }

        [Test]
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
                    Assert.IsTrue(reader.Read());

                    // act
                    var allUnstructuredTypesObject = reader.GetObject<AllNullableUnstructuredTypesClass>(0);

                    // assert
                    Assert.NotNull(allUnstructuredTypesObject);
                    Assert.IsNull(allUnstructuredTypesObject.StringValue);
                    Assert.IsNull(allUnstructuredTypesObject.CharValue);
                    Assert.IsNull(allUnstructuredTypesObject.ByteValue);
                    Assert.IsNull(allUnstructuredTypesObject.SByteValue);
                    Assert.IsNull(allUnstructuredTypesObject.ShortValue);
                    Assert.IsNull(allUnstructuredTypesObject.UShortValue);
                    Assert.IsNull(allUnstructuredTypesObject.IntValue);
                    Assert.IsNull(allUnstructuredTypesObject.UIntValue);
                    Assert.IsNull(allUnstructuredTypesObject.LongValue);
                    Assert.IsNull(allUnstructuredTypesObject.ULongValue);
                    Assert.IsNull(allUnstructuredTypesObject.FloatValue);
                    Assert.IsNull(allUnstructuredTypesObject.DoubleValue);
                    Assert.IsNull(allUnstructuredTypesObject.DecimalValue);
                    Assert.IsNull(allUnstructuredTypesObject.BooleanValue);
                    Assert.IsNull(allUnstructuredTypesObject.GuidValue);
                    Assert.IsNull(allUnstructuredTypesObject.DateTimeValue);
                    Assert.IsNull(allUnstructuredTypesObject.DateTimeOffsetValue);
                    Assert.IsNull(allUnstructuredTypesObject.TimeSpanValue);
                    Assert.IsNull(allUnstructuredTypesObject.BinaryValue);
                    Assert.IsNull(allUnstructuredTypesObject.SemiStructuredValue);
                }
            }
        }

        [Test]
        [TestCaseSource(nameof(DateTimeConversionCases))]
        public void TestSelectDateTime(string dbValue, string dbType, DateTime? expectedRaw, DateTime expected)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    SetTimePrecision(connection, 9);
                    var rawValueString = $"'{dbValue}'::{dbType}";
                    var objectValueString = $"OBJECT_CONSTRUCT('Value', {rawValueString})::OBJECT(Value {dbType})";
                    command.CommandText = $"SELECT {rawValueString}, {objectValueString}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act/assert
                    if (expectedRaw != null)
                    {
                        var rawValue = reader.GetDateTime(0);
                        Assert.AreEqual(expectedRaw, rawValue);
                        Assert.AreEqual(expectedRaw?.Kind, rawValue.Kind);
                    }
                    var wrappedValue = reader.GetObject<DateTimeWrapper>(1);
                    Assert.AreEqual(expected, wrappedValue.Value);
                    Assert.AreEqual(expected.Kind, wrappedValue.Value.Kind);
                }
            }
        }

        internal static IEnumerable<object[]> DateTimeConversionCases()
        {
            yield return new object[] { "2024-07-11 14:20:05", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime() };
            yield return new object[] { "2024-07-11 14:20:05 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), null, DateTime.Parse("2024-07-11 09:20:05").ToUniversalTime() };
            yield return new object[] {"2024-07-11 14:20:05 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), null, DateTime.Parse("2024-07-11 21:20:05").ToUniversalTime() };
            yield return new object[] { "2024-07-11", SFDataType.DATE.ToString(), DateTime.Parse("2024-07-11").ToUniversalTime(), DateTime.Parse("2024-07-11").ToUniversalTime() };
            yield return new object[] { "2024-07-11 14:20:05.123456789", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05.1234567").ToUniversalTime(), DateTime.Parse("2024-07-11 14:20:05.1234568").ToUniversalTime()};
            yield return new object[] { "2024-07-11 14:20:05.123456789 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), null, DateTime.Parse("2024-07-11 09:20:05.1234568").ToUniversalTime() };
            yield return new object[] {"2024-07-11 14:20:05.123456789 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), null, DateTime.Parse("2024-07-11 21:20:05.1234568").ToUniversalTime() };
        }

        [Test]
        [TestCaseSource(nameof(DateTimeOffsetConversionCases))]
        public void TestSelectDateTimeOffset(string dbValue, string dbType, DateTime? expectedRaw, DateTimeOffset expected)
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    EnableStructuredTypes(connection);
                    SetTimePrecision(connection, 9);
                    var rawValueString = $"'{dbValue}'::{dbType}";
                    var objectValueString = $"OBJECT_CONSTRUCT('Value', {rawValueString})::OBJECT(Value {dbType})";
                    command.CommandText = $"SELECT {rawValueString}, {objectValueString}";
                    var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // act/assert
                    if (expectedRaw != null)
                    {
                        var rawValue = reader.GetDateTime(0);
                        Assert.AreEqual(expectedRaw, rawValue);
                        Assert.AreEqual(expectedRaw?.Kind, rawValue.Kind);
                    }
                    var wrappedValue = reader.GetObject<DateTimeOffsetWrapper>(1);
                    Assert.AreEqual(expected, wrappedValue.Value);
                }
            }
        }

        internal static IEnumerable<object[]> DateTimeOffsetConversionCases()
        {
            yield return new object[] {"2024-07-11 14:20:05", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime(), DateTimeOffset.Parse("2024-07-11 14:20:05Z")};
            yield return new object[] {"2024-07-11 14:20:05 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05 +5:00")};
            yield return new object[] {"2024-07-11 14:20:05 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05 -7:00")};
            yield return new object[] {"2024-07-11", SFDataType.DATE.ToString(), DateTime.Parse("2024-07-11").ToUniversalTime(), DateTimeOffset.Parse("2024-07-11Z")};
            yield return new object[] {"2024-07-11 14:20:05.123456789", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05.1234567").ToUniversalTime(), DateTimeOffset.Parse("2024-07-11 14:20:05.1234568Z")};
            yield return new object[] {"2024-07-11 14:20:05.123456789 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05.1234568 +5:00")};
            yield return new object[] {"2024-07-11 14:20:05.123456789 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), null, DateTimeOffset.Parse("2024-07-11 14:20:05.1234568 -7:00")};
        }

        private TimeZoneInfo GetTimeZone(SnowflakeDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "show parameters like 'timezone'";
                var reader = (SnowflakeDbDataReader) command.ExecuteReader();
                Assert.IsTrue(reader.Read());
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
