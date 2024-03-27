/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFBindUploaderTest
    {
        private readonly SFBindUploader _bindUploader = new SFBindUploader(null, "test");

        [TestCase(SFDataType.DATE, "0", "1/1/1970")]
        [TestCase(SFDataType.DATE, "73814400000", "5/4/1972")]
        [TestCase(SFDataType.DATE, "1709193600000", "2/29/2024")]
        public void TestCsvDataConversionForDate(SFDataType dbType, string input, string expected)
        {
            DateTime dateExpected = DateTime.Parse(expected);
            DateTime dateActual = DateTime.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            Assert.AreEqual(dateExpected, dateActual);
        }

        [TestCase(SFDataType.TIME, "0", "00:00:00.000000")]
        [TestCase(SFDataType.TIME, "100000000", "00:00:00.100000")]
        [TestCase(SFDataType.TIME, "1000000000", "00:00:01.000000")]
        [TestCase(SFDataType.TIME, "60123456000", "00:01:00.123456")]
        [TestCase(SFDataType.TIME, "46801000000000", "13:00:01.000000")]
        public void TestCsvDataConversionForTime(SFDataType dbType, string input, string expected)
        {
            DateTime timeExpected = DateTime.Parse(expected);
            DateTime timeActual = DateTime.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            Assert.AreEqual(timeExpected, timeActual);
        }
        
        [TestCase(SFDataType.TIMESTAMP_LTZ, "0", "2012-07-01T12:00:00.0000000+02:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "1341144000000000000", "2012-07-01T12:00:00.0000000+02:00")]
        [TestCase(SFDataType.TIMESTAMP_LTZ, "352245599987654000", "1981-02-28T23:59:59.9876540+02:00")]
        public void TestCsvDataConversionForTimestampLtz(SFDataType dbType, string input, string expected)
        {
            DateTimeOffset timestampExpected = DateTimeOffset.Parse(expected);
            DateTimeOffset timestampActual = DateTimeOffset.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            Assert.AreEqual(timestampExpected, timestampActual);
        }
        
        [TestCase(SFDataType.TIMESTAMP_TZ, "1341136800000000000 1560", "2012-07-01 12:00:00.000000 +02:00")]
        [TestCase(SFDataType.TIMESTAMP_TZ, "352245599987654000 1560", "1981-02-28 23:59:59.987654 +02:00")]
        public void TestCsvDataConversionForTimestampTz(SFDataType dbType, string input, string expected)
        {
            DateTimeOffset timestampExpected = DateTimeOffset.Parse(expected);
            DateTimeOffset timestampActual = DateTimeOffset.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
            Assert.AreEqual(timestampExpected, timestampActual);
        }
        
        [TestCase(SFDataType.TIMESTAMP_NTZ, "", "2012-07-01 12:00:00.000000 +02:00")]
        [TestCase(SFDataType.TIMESTAMP_NTZ, "", "1981-02-28 23:59:59.987654 +02:00")]
        public void TestCsvDataConversionForTimestampNtz(SFDataType dbType, string input, string expected)
        {
            DateTimeOffset timestampExpected = DateTimeOffset.Parse(expected);
            DateTimeOffset timestampActual = DateTimeOffset.Parse(_bindUploader.GetCSVData(dbType.ToString(), input));
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
            Assert.AreEqual(expected, _bindUploader.GetCSVData(dbType.ToString(), input));
        }

    }
}
