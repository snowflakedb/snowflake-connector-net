/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;

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

        private static readonly DateTime[] _testConvertDatetimeInputData =
        {
            new DateTime(2019, 2, 4, 15, 30, 1, 123),
            new DateTime(1982, 1, 18, 16, 20, 00, 666),
            /* This test and conversion will fail if not-even-seconds before unix epoch are used.
            new DateTime(1900, 9, 3).AddTicks(1), */
            new DateTime(2100, 1, 1, 1, 1, 1, 1).AddTicks(1)
        };

        [Test]
        [TestCase("2100-12-31 23:59:59.9999999")]
        //[TestCase("9999-12-31 23:59:59.9999999")] fails
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
    }
}
