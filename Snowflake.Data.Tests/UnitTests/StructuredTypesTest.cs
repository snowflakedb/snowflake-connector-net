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
            var sfType = (SFDataType) Enum.Parse(typeof(SFDataType), sfTypeString);
            var csharpType = expected.GetType();

            // act
            var result = timeConverter.Convert(value, sfType, csharpType);

            // assert
            Assert.AreEqual(expected, result);
        }

        internal static IEnumerable<object[]> TimeConversionCases()
        {
            yield return new object[] {"2024-07-11 14:20:05", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05").ToUniversalTime()};
            yield return new object[] {"2024-07-11 14:20:05", SFDataType.TIMESTAMP_NTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05Z")};
            yield return new object[] {"2024-07-11 14:20:05 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05 +5:00")};
            yield return new object[] {"2024-07-11 14:20:05 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTime.Parse("2024-07-11 09:20:05").ToUniversalTime()};
            yield return new object[] {"2024-07-11 14:20:05 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05 -7:00")};
            yield return new object[] {"2024-07-11 14:20:05 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTime.Parse("2024-07-11 21:20:05").ToUniversalTime()};
            yield return new object[] {"14:20:05", SFDataType.TIME.ToString(), TimeSpan.Parse("14:20:05")};
            yield return new object[] {"2024-07-11", SFDataType.DATE.ToString(), DateTime.Parse("2024-07-11")};
            yield return new object[] {"2024-07-11 14:20:05.123456", SFDataType.TIMESTAMP_NTZ.ToString(), DateTime.Parse("2024-07-11 14:20:05.123456").ToUniversalTime()};
            yield return new object[] {"2024-07-11 14:20:05.123456", SFDataType.TIMESTAMP_NTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05.123456Z")};
            yield return new object[] {"2024-07-11 14:20:05.123456 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05.123456 +5:00")};
            yield return new object[] {"2024-07-11 14:20:05.123456 +5:00", SFDataType.TIMESTAMP_TZ.ToString(), DateTime.Parse("2024-07-11 09:20:05.123456").ToUniversalTime()};
            yield return new object[] {"2024-07-11 14:20:05.123456 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTimeOffset.Parse("2024-07-11 14:20:05.123456 -7:00")};
            yield return new object[] {"2024-07-11 14:20:05.123456 -7:00", SFDataType.TIMESTAMP_LTZ.ToString(), DateTime.Parse("2024-07-11 21:20:05.123456").ToUniversalTime()};
            yield return new object[] {"14:20:05.123456", SFDataType.TIME.ToString(), TimeSpan.Parse("14:20:05.123456")};
        }
    }
}
