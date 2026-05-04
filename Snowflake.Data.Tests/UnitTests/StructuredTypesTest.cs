using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Converter;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class StructuredTypesTest
    {
        [Test]
        [TestCaseSource(nameof(TimeConversionCases))]
        public void TestTimeConversions(string value, string sfTypeString, object expected)
        {
            // arrange
            var timeConverter = new TimeConverter();
            var sfType = (SFDataType)Enum.Parse(typeof(SFDataType), sfTypeString);
            var csharpType = expected.GetType();

            // act
            var result = timeConverter.Convert(value, sfType, csharpType, TimeZoneInfo.Local);

            // assert
            Assert.AreEqual(expected, result);

            if (csharpType == typeof(DateTime))
            {
                Assert.AreEqual(((DateTime)expected).Kind, ((DateTime)result).Kind);
            }
        }

        internal static IEnumerable<object[]> TimeConversionCases()
        {
            yield return new object[] { "2024-07-11 14:20:05", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05") };
            yield return new object[] { "2024-07-11 14:20:05", SFDataType.TIMESTAMP_NTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05Z") };
            yield return new object[] { "2024-07-11 14:20:05 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05 +5:00") };
            yield return new object[] { "2024-07-11 14:20:05 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTime.SpecifyKind(DateTime.Parse("2024-07-11 09:20:05"), DateTimeKind.Utc) };
            yield return new object[] { "2024-07-11 14:20:05 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05 -7:00") };
            yield return new object[] { "2024-07-11 14:20:05 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTime.Parse("2024-07-11 21:20:05").ToLocalTime() };
            yield return new object[] { "14:20:05", SFDataType.TIME.ToString(), TimeSpan.Parse("14:20:05") };
            yield return new object[] { "2024-07-11", SFDataType.DATE.ToString(), DateTime.Parse("2024-07-11") };
            yield return new object[] { "2024-07-11 14:20:05.123456", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05.123456") };
            yield return new object[] { "2024-07-11 14:20:05.123456", SFDataType.TIMESTAMP_NTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05.123456Z") };
            yield return new object[] { "2024-07-11 14:20:05.123456 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05.123456 +5:00") };
            yield return new object[] { "2024-07-11 14:20:05.123456 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTime.SpecifyKind(DateTime.Parse("2024-07-11 09:20:05.123456"), DateTimeKind.Utc) };
            yield return new object[] { "2024-07-11 14:20:05.123456 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05.123456 -7:00") };
            yield return new object[] { "2024-07-11 14:20:05.123456 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTime.Parse("2024-07-11 21:20:05.123456").ToLocalTime() };
            yield return new object[] { "14:20:05.123456", SFDataType.TIME.ToString(), TimeSpan.Parse("14:20:05.123456") };
            yield return new object[] { "9999-12-31 23:59:59.999999", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("9999-12-31 23:59:59.999999") };
            yield return new object[] { "9999-12-31 23:59:59.999999", SFDataType.TIMESTAMP_NTZ.ToString(), DateTimeOffset.Parse("9999-12-31 23:59:59.999999Z") };
            yield return new object[] { "9999-12-31 23:59:59.999999 +1:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTimeOffset.Parse("9999-12-31 23:59:59.999999 +1:00") };
            yield return new object[] { "9999-12-31 23:59:59.999999 +1:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTime.SpecifyKind(DateTime.Parse("9999-12-31 22:59:59.999999"), DateTimeKind.Utc) };
            yield return new object[] { "9999-12-31 23:59:59.999999 +1:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTimeOffset.Parse("9999-12-31 23:59:59.999999 +1:00") };
            yield return new object[] { "9999-12-31 23:59:59.999999 +13:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTime.Parse("9999-12-31 10:59:59.999999").ToLocalTime() };
            yield return new object[] { "0001-01-01 00:00:00.123456", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("0001-01-01 00:00:00.123456") };
            yield return new object[] { "0001-01-01 00:00:00.123456", SFDataType.TIMESTAMP_NTZ.ToString(), DateTimeOffset.Parse("0001-01-01 00:00:00.123456Z") };
            yield return new object[] { "0001-01-01 00:00:00.123456 -1:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTimeOffset.Parse("0001-01-01 00:00:00.123456 -1:00") };
            yield return new object[] { "0001-01-01 00:00:00.123456 -1:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTime.SpecifyKind(DateTime.Parse("0001-01-01 01:00:00.123456"), DateTimeKind.Utc) };
            yield return new object[] { "0001-01-01 00:00:00.123456 -1:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTimeOffset.Parse("0001-01-01 00:00:00.123456 -1:00") };
            yield return new object[] { "0001-01-01 00:00:00.123456 -13:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTime.Parse("0001-01-01 13:00:00.123456").ToLocalTime() };
        }

        [Test]
        [TestCaseSource(nameof(TimeConversionWithNamedTimezoneCases))]
        public void TestTimeConversionsWithNamedTimezone(string value, string sfTypeString, string tzName, object expected)
        {
            var timeConverter = new TimeConverter();
            var sfType = (SFDataType)Enum.Parse(typeof(SFDataType), sfTypeString);
            var csharpType = expected.GetType();
            var tz = TimeZoneConverter.TZConvert.GetTimeZoneInfo(tzName);

            var result = timeConverter.Convert(value, sfType, csharpType, tz);

            Assert.AreEqual(expected, result);
            if (csharpType == typeof(DateTime))
            {
                Assert.AreEqual(((DateTime)expected).Kind, ((DateTime)result).Kind);
            }
            if (csharpType == typeof(DateTimeOffset))
            {
                Assert.AreEqual(((DateTimeOffset)expected).Offset, ((DateTimeOffset)result).Offset);
            }
        }

        internal static IEnumerable<object[]> TimeConversionWithNamedTimezoneCases()
        {
            // TIMESTAMP_LTZ -> DateTimeOffset with Asia/Tokyo (UTC+9, no DST)
            yield return new object[]
            {
                "2024-07-11 14:20:05 -7:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                "Asia/Tokyo",
                new DateTimeOffset(2024, 7, 12, 6, 20, 5, TimeSpan.FromHours(9))
            };

            // TIMESTAMP_LTZ -> DateTime with Asia/Tokyo
            yield return new object[]
            {
                "2024-07-11 14:20:05 -7:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                "Asia/Tokyo",
                DateTime.SpecifyKind(new DateTime(2024, 7, 12, 6, 20, 5), DateTimeKind.Local)
            };

            // TIMESTAMP_LTZ -> DateTimeOffset with Europe/Warsaw (CEST = UTC+2 in July)
            yield return new object[]
            {
                "2024-07-15 10:30:00 +0:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                "Europe/Warsaw",
                new DateTimeOffset(2024, 7, 15, 12, 30, 0, TimeSpan.FromHours(2))
            };

            // TIMESTAMP_LTZ -> DateTime with Europe/Warsaw (CET = UTC+1 in January)
            yield return new object[]
            {
                "2024-01-15 10:30:00 +0:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                "Europe/Warsaw",
                DateTime.SpecifyKind(new DateTime(2024, 1, 15, 11, 30, 0), DateTimeKind.Local)
            };

            // TIMESTAMP_LTZ -> DateTimeOffset with UTC
            yield return new object[]
            {
                "2024-01-01 00:00:00 +5:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                "UTC",
                new DateTimeOffset(2023, 12, 31, 19, 0, 0, TimeSpan.Zero)
            };

            // TIMESTAMP_LTZ -> DateTimeOffset with fractional seconds
            yield return new object[]
            {
                "2024-07-11 14:20:05.123456 -2:00",
                SFDataType.TIMESTAMP_LTZ.ToString(),
                "Asia/Tokyo",
                new DateTimeOffset(2024, 7, 12, 1, 20, 5, 123, TimeSpan.FromHours(9)).AddTicks(4560)
            };
        }

        [Test]
        public void TestTimeConverterThrowsWhenSessionTimezoneIsNull()
        {
            var timeConverter = new TimeConverter();

            var ex = Assert.Throws<StructuredTypesReadingException>(() =>
                timeConverter.Convert("2024-01-01 00:00:00 +0:00", SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), null));

            Assert.That(ex.Message, Does.Contain("Session timezone is required"));
        }

        [Test]
        public void TestTimeConverterLtzReturnsStringWhenTargetIsString()
        {
            var timeConverter = new TimeConverter();
            var tokyoTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Asia/Tokyo");

            var result = timeConverter.Convert("2024-01-01 00:00:00 +0:00", SFDataType.TIMESTAMP_LTZ, typeof(string), tokyoTz);

            Assert.AreEqual("2024-01-01 00:00:00 +0:00", result);
        }
    }
}
