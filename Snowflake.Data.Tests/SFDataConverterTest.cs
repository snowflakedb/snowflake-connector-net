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
        [TestCase("0", false)]
        [TestCase("t", true)]
        [TestCase("T", true)]
        [TestCase("1", true)]
        [TestCase("anything else", false)]
        public void TestConvertBoolean(string inputBooleanString, bool expected)
        {
            var actual = SFDataConverter.ConvertToCSharpVal(inputBooleanString, SFDataType.BOOLEAN, typeof(bool));
            Assert.AreEqual(expected, actual);
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
        [TestCase("2147483647")]
        [TestCase("-2147483648")]
        [TestCase("-1")]
        [TestCase("0")]
        public void TestConvertToInt32(string s)
        {
            Int32 actual = (Int32)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int32));
            Int32 expected = Convert.ToInt32(s);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("32767")]
        [TestCase("-32768")]
        [TestCase("-1")]
        [TestCase("0")]
        public void TestConvertToInt16(string s)
        {
            Int16 actual = (Int16)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int16));
            Int16 expected = Convert.ToInt16(s);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("255")]
        [TestCase("0")]
        public void TestConvertToByte(string s)
        {
            byte actual = (byte)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(byte));
            byte expected = Convert.ToByte(s);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("256")]
        [TestCase("-1")]
        public void TestOverflowByte(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(byte)));
        }

        [Test]
        [TestCase("32768")]
        [TestCase("-32769")]
        public void TestOverflowInt16(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int16)));
        }

        [Test]
        [TestCase("2147483648")]
        [TestCase("-2147483649")]
        public void TestOverflowInt32(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int32)));
        }

        [Test]
        [TestCase("9223372036854775808")]
        [TestCase("-9223372036854775809")]
        public void TestOverflowInt64(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int64)));
        }

        [Test]
        [TestCase("9223372036854775807.9223372036854775807")]
        [TestCase("-9223372036854775807.1234567890")]
        [TestCase("-1.300")]
        [TestCase("999999999999999999.000000000000100000000000")]
        [TestCase("4294967295.4294967296")]
        [TestCase("-0.999")]
        [TestCase("307.48100000000000000000")]
        [TestCase("79228162514264337593543950335")] // Max decimal value
        [TestCase("-79228162514264337593543950335")] // Min decimal value
        [TestCase("9.9999999999999999999999999999")] // The scaling factor range is 0 to 28
        [TestCase("-9.9999999999999999999999999999")] // The scaling factor range is 0 to 28
        [TestCase("79228162514264337593543950334.9999999999999999999999999999")] //A Decimal object has 29 digits of precision. If s represents a number that has more than 29 digits, but has a fractional part and is within the range of MaxValue and MinValue, the number is rounded
        public void TestConvertToDecimal(string s)
        {
            decimal actual = (decimal)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(decimal));
            decimal expected = Convert.ToDecimal(s, CultureInfo.InvariantCulture);

            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("79228162514264337593543950336")] // Max decimal value + 1
        [TestCase("-79228162514264337593543950336")] // Min decimal value - 1
        [TestCase("79228162514264337593543950335.9999999999999999999999999999")] // The scaling factor range is 0 to 28. Scaling factor = 29 and fractional part > MaxValue
        public void TestOverflowDecimal(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(decimal)));
        }

        [Test]
        [TestCase("9223372036854775807.9223372036854775807")]
        [TestCase("-9223372036854775807.1234567890")]
        [TestCase("-1.300")]
        [TestCase("-0.999")]
        [TestCase("999999999999999999.000000000000100000000000")]
        [TestCase("4294967295.4294967296")]
        [TestCase("1.5e-36")]
        [TestCase("1.5e+38")]
        //[TestCase("inf")] -- TODO - Not supported yet
        //[TestCase("-inf")] -- TODO - Not supported yet
        [TestCase("NaN")]
        public void TestConvertToFloat(string s)
        {
            double actualDouble = (double)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(double));
            double expectedDoulbe = Convert.ToDouble(s, CultureInfo.InvariantCulture);

            Assert.AreEqual(actualDouble, expectedDoulbe);

            float actualFloat = (float)SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(float));
            float expectedFloat = Convert.ToSingle(s, CultureInfo.InvariantCulture);

            Assert.AreEqual(expectedFloat, actualFloat);
        }

        [Test]
        [TestCase("thisIsNotAValidValue")]
        [TestCase("-0.999")]
        [TestCase("-1.300")]
        [TestCase("425.426")]
        [TestCase("1.5e-36")]
        [TestCase("1.5e+38")]
        [TestCase("inf")]
        [TestCase("-inf")]
        [TestCase("NaN")]
        public void TestInvalidConversionInvalidInt(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int32)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int64)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(Int16)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(byte)));
        }

        [Test]
        [TestCase("thisIsNotAValidValue")]
        public void TestInvalidConversionInvalidFloat(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(float)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(double)));
        }

        [Test]
        [TestCase("thisIsNotAValidValue")]
        [TestCase("1.5e-36")]
        [TestCase("1.5e+38")]
        [TestCase("inf")]
        [TestCase("-inf")]
        [TestCase("NaN")]
        public void TestInvalidConversionInvalidDecimal(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(s, SFDataType.FIXED, typeof(decimal)));
        }

    }
}
