using System;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    [SetCulture("en-US")]
    class SFBindUploaderTest
    {
        private readonly SFBindUploader _bindUploader = new SFBindUploader(null, "test");

        [TestCase(SFDataType.DATE, "0", "1/1/1970")]
        [TestCase(SFDataType.DATE, "73785600000", "5/4/1972")]
        [TestCase(SFDataType.DATE, "1709164800000", "2/29/2024")]
        public void TestCsvDataConversionForDate(SFDataType dbType, string input, string expected)
        {
            // Arrange
            var dateExpected = DateTime.Parse(expected);
            var check = SFDataConverter.CSharpValToSfVal(SFDataType.DATE, dateExpected);
            Assert.AreEqual(check, input);
            // Act
            DateTime dateActual = DateTime.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            // Assert
            Assert.AreEqual(dateExpected, dateActual);
        }

        [TestCase(SFDataType.TIME, "0", "00:00:00.000000")]
        [TestCase(SFDataType.TIME, "100000000", "00:00:00.100000")]
        [TestCase(SFDataType.TIME, "1000000000", "00:00:01.000000")]
        [TestCase(SFDataType.TIME, "60123456000", "00:01:00.123456")]
        [TestCase(SFDataType.TIME, "46801000000000", "13:00:01.000000")]
        public void TestCsvDataConversionForTime(SFDataType dbType, string input, string expected)
        {
            // Arrange
            DateTime timeExpected = DateTime.Parse(expected);
            var check = SFDataConverter.CSharpValToSfVal(SFDataType.TIME, timeExpected);
            Assert.AreEqual(check, input);
            // Act
            DateTime timeActual = DateTime.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            // Assert
            Assert.AreEqual(timeExpected, timeActual);
        }

        [TestCase(SFDataType.TIMESTAMP_LTZ, "0", "1970-01-01T00:00:00.0000000+00:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "39600000000000", "1970-01-01T12:00:00.0000000+01:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "1341136800000000000", "2012-07-01T12:00:00.0000000+02:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "352245599987654000", "1981-02-28T23:59:59.9876540+02:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "1678868249207000000", "2023/03/15T13:17:29.207+05:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "253402300799999999900", "9999-12-31T23:59:59.9999999+00:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "-62135596800000000000", "0001-01-01T00:00:00.0000000+00:00")]
        public void TestCsvDataConversionForTimestampLtz(SFDataType dbType, string input, string expected)
        {
            // Arrange
            var timestampExpected = DateTimeOffset.Parse(expected);
            var check = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_LTZ, timestampExpected);
            Assert.AreEqual(check, input);
            // Act
            var timestampActual = DateTimeOffset.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            // Assert
            Assert.AreEqual(timestampExpected.ToLocalTime(), timestampActual);
        }

        [TestCase(SFDataType.TIMESTAMP_TZ, "0 1440", "1970-01-01 00:00:00.000000 +00:00")]
        [TestCase(SFDataType.TIMESTAMP_TZ, "1341136800000000000 1560", "2012-07-01 12:00:00.000000 +02:00")]
        [TestCase(SFDataType.TIMESTAMP_TZ, "352245599987654000 1560", "1981-02-28 23:59:59.987654 +02:00")]
        [TestCase(SFDataType.TIMESTAMP_TZ, "253402300799999999000 1440", "9999-12-31 23:59:59.999999 +00:00")]
        [TestCase(SFDataType.TIMESTAMP_TZ, "-62135596800000000000 1440", "0001-01-01 00:00:00.000000 +00:00")]
        public void TestCsvDataConversionForTimestampTz(SFDataType dbType, string input, string expected)
        {
            // Arrange
            DateTimeOffset timestampExpected = DateTimeOffset.Parse(expected);
            var check = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_TZ, timestampExpected);
            Assert.AreEqual(check, input);
            // Act
            DateTimeOffset timestampActual = DateTimeOffset.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            // Assert
            Assert.AreEqual(timestampExpected, timestampActual);
        }

        [TestCase(SFDataType.TIMESTAMP_NTZ, "0", "1970-01-01 00:00:00.000000")]
        [TestCase(SFDataType.TIMESTAMP_NTZ, "1341144000000000000", "2012-07-01 12:00:00.000000")]
        [TestCase(SFDataType.TIMESTAMP_NTZ, "352252799987654000", "1981-02-28 23:59:59.987654")]
        [TestCase(SFDataType.TIMESTAMP_NTZ, "253402300799999999000", "9999-12-31 23:59:59.999999")]
        [TestCase(SFDataType.TIMESTAMP_NTZ, "-62135596800000000000", "0001-01-01 00:00:00.000000")]
        public void TestCsvDataConversionForTimestampNtz(SFDataType dbType, string input, string expected)
        {
            // Arrange
            DateTime timestampExpected = DateTime.Parse(expected);
            var check = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_NTZ, timestampExpected);
            Assert.AreEqual(check, input);
            // Act
            DateTime timestampActual = DateTime.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            // Assert
            Assert.AreEqual(timestampExpected, timestampActual);
        }

        [TestCase(SFDataType.TEXT, "", "\"\"")]
        [TestCase(SFDataType.TEXT, "\"", "\"\"\"\"")]
        [TestCase(SFDataType.TEXT, "\n", "\"\n\"")]
        [TestCase(SFDataType.TEXT, "\t", "\"\t\"")]
        [TestCase(SFDataType.TEXT, ",", "\",\"")]
        [TestCase(SFDataType.TEXT, "Sample text", "Sample text")]
        public void TestCsvDataConversionForText(SFDataType dbType, string input, string expected)
        {
            // Act
            var actual = _bindUploader.GetCSVData(dbType.ToString(), input);
            // Assert
            Assert.AreEqual(expected, actual);
        }

    }
}
