/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using System.Threading;
    using System.Globalization;

    [TestFixture]
    class SFDataConverterTest
    {
        [Test]
        public void TestConvertBindToSFValFinlandLocale()
        {
            Thread testThread = new Thread(() =>
            {
                CultureInfo ci = new CultureInfo("en-FI");

                Thread.CurrentThread.CurrentCulture = ci;

                System.Tuple<string, string> t = 
                    SFDataConverter.csharpTypeValToSfTypeVal(System.Data.DbType.Double, 1.2345);

                Assert.AreEqual("REAL", t.Item1);
                Assert.AreEqual("1.2345", t.Item2);
            });
            testThread.Start();
            testThread.Join();
        }

        [Test]
        [TestCase("2100-12-31 23:59:59.9999999")]
        [TestCase("2200-01-01 11:22:33.4455667")]
        [TestCase("9999-12-31 23:59:59.9999999")]
        [TestCase("1982-01-18 16:20:00.6666666")]
        [TestCase(null)]
        public void TestConvertDatetime(string inputTimeStr)
        {
            DateTime inputTime;
            if (inputTimeStr == null)
            {
                inputTime = DateTime.Now;
            }
            else
            {
                inputTime = DateTime.ParseExact(inputTimeStr, "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            }

            var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var tickDiff = inputTime.Ticks - unixEpoch.Ticks;
            var inputStringAsItWasFromDatabase = (tickDiff / 10000000.0m).ToString(CultureInfo.InvariantCulture);
            var result = SFDataConverter.ConvertToCSharpVal(inputStringAsItWasFromDatabase, SFDataType.TIMESTAMP_NTZ, typeof(DateTime));
            Assert.AreEqual(inputTime, result);
        }

        [Test]
        [TestCase("2100-12-31 23:59:59.9999999", DateTimeKind.Utc)]
        [TestCase("2100-12-31 23:59:59.9999999", DateTimeKind.Local)]
        [TestCase("2100-12-31 23:59:59.9999999", DateTimeKind.Unspecified)]
        [TestCase("2200-01-01 00:00:00.0000000", DateTimeKind.Utc)]
        [TestCase("2200-01-01 00:00:00.0000000", DateTimeKind.Local)]
        [TestCase("2200-01-01 00:00:00.0000000", DateTimeKind.Unspecified)]
        [TestCase("1960-01-01 00:00:00.0000000", DateTimeKind.Unspecified)]
        [TestCase("9999-12-31 23:59:59.9999999", DateTimeKind.Unspecified)]
        [TestCase("1982-01-18 16:20:00.6666666", DateTimeKind.Unspecified)]
        [TestCase("1982-01-18 23:59:59.0000000", DateTimeKind.Unspecified)]
        [TestCase(null, DateTimeKind.Unspecified)]
        public void TestConvertDate(string inputTimeStr, object kind = null)
        {
            if (kind == null)
                kind = 0;
            DateTime inputTime;
            if (inputTimeStr == null)
            {
                inputTime = DateTime.Now;
            }
            else
            {
                inputTime = DateTime.ParseExact(inputTimeStr, "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            }
            var dtExpected = inputTime.Date;
            internalTestConvertDate(dtExpected, DateTime.SpecifyKind(inputTime, (DateTimeKind)kind));
        }

        private void internalTestConvertDate(DateTime dtExpected, DateTime testValue)
        {
            var result = SFDataConverter.csharpTypeValToSfTypeVal(System.Data.DbType.Date, testValue);
            // Convert result to DateTime for easier interpretation
            var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            DateTime dtResult = unixEpoch.AddMilliseconds(Int64.Parse(result.Item2));
            Assert.AreEqual(dtExpected, dtResult);
        }

        [Test]
        [TestCase("9223372036854775807")]
        [TestCase("-9223372036854775808")]
        [TestCase("-1")]
        [TestCase("999999999999999999")]
        public void TestConvertToInt64(string s)
        {
            Int64 actual = (Int64)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int64));
            Int64 expected = Convert.ToInt64(s);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("9223372036854775807.9223372036854775807")]
        [TestCase("-9223372036854775807.1234567890")]
        [TestCase("-1.300")]
        [TestCase("999999999999999999.000000000000100000000000")]
        [TestCase("4294967295.4294967296")]
        public void TestConvertToDecimal(string s)
        {
            decimal actual = (decimal)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(decimal));
            decimal expected = Convert.ToDecimal(s, System.Globalization.CultureInfo.InvariantCulture);
            Assert.AreEqual(expected, actual);
        }
    }
}
