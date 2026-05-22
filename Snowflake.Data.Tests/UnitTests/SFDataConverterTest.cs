using System;
using System.Data;
using System.Text;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using System.Threading;
    using System.Globalization;

    [TestFixture]
    [SetCulture("en-US")]
    public sealed class SFDataConverterTest
    {
        // Method with the same signature as before the performance work
        // Used by unit tests only
        private UTF8Buffer ConvertToUTF8Buffer(string srcVal)
        {
            // Create an UTF8Buffer with an offset to get better testing
            byte[] b1 = Encoding.UTF8.GetBytes(srcVal);
            byte[] b2 = new byte[b1.Length + 100];
            Array.Copy(b1, 0, b2, 100, b1.Length);
            return new UTF8Buffer(b2, 100, b1.Length);
        }

        [Test]
        public void TestConvertBindToSFValFinlandLocale()
        {
            Thread testThread = new Thread(() =>
            {
                CultureInfo ci = new CultureInfo("en-FI");

                Thread.CurrentThread.CurrentCulture = ci;

                System.Tuple<string, string> t =
                    SFDataConverter.CSharpTypeValToSfTypeVal(System.Data.DbType.Double, 1.2345);

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
            var actual = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(inputBooleanString), SFDataType.BOOLEAN, typeof(bool));
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("2100-12-31 23:59:59.9999999")]
        [TestCase("2200-01-01 11:22:33.4455667")]
        [TestCase("9999-12-31 23:59:59.9999999")]
        [TestCase("1982-01-18 16:20:00.6666666")]
        [TestCase("1969-12-31 23:59:59.1234567")]
        [TestCase("1960-06-15 10:30:45.5000000")]
        [TestCase("1900-01-01 00:00:00.0000001")]
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
            var result = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(inputStringAsItWasFromDatabase), SFDataType.TIMESTAMP_NTZ, typeof(DateTime));
            Assert.AreEqual(inputTime, result);
        }

        [Test]
        [TestCase("11:22:33.4455667")]
        [TestCase("23:59:59.9999999")]
        [TestCase("16:20:00.6666666")]
        [TestCase("00:00:00.0000000")]
        [TestCase("00:00:00")]
        [TestCase("23:59:59.1")]
        [TestCase("23:59:59.12")]
        [TestCase("23:59:59.123")]
        [TestCase("23:59:59.1234")]
        [TestCase("23:59:59.12345")]
        [TestCase("23:59:59.123456")]
        [TestCase("23:59:59.1234567")]
        [TestCase("23:59:59.12345678")]
        [TestCase("23:59:59.123456789")]
        public void TestConvertTimeSpan(string inputTimeStr)
        {
            // The expected result. Timespan precision only goes up to 7 digits
            TimeSpan expected = TimeSpan.ParseExact(inputTimeStr.Length < 16 ? inputTimeStr : inputTimeStr.Substring(0, 16), "c", CultureInfo.InvariantCulture);

            // Generate the value as returned by the DB
            TimeSpan val = TimeSpan.ParseExact(inputTimeStr.Substring(0, 8), "c", CultureInfo.InvariantCulture);
            Console.WriteLine("val " + val.ToString());
            var tickDiff = val.Ticks;
            var inputStringAsItComesBackFromDatabase = (tickDiff / 10000000.0m).ToString(CultureInfo.InvariantCulture);
            inputStringAsItComesBackFromDatabase += inputTimeStr.Substring(8, inputTimeStr.Length - 8);

            // Run the conversion
            var result = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(inputStringAsItComesBackFromDatabase), SFDataType.TIME, typeof(TimeSpan));

            // Verify the result
            Assert.AreEqual(expected, result);
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
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(System.Data.DbType.Date, testValue);
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
            Int64 actual = (Int64)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int64));
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
            Int32 actual = (Int32)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int32));
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
            Int16 actual = (Int16)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int16));
            Int16 expected = Convert.ToInt16(s);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("255")]
        [TestCase("0")]
        public void TestConvertToByte(string s)
        {
            byte actual = (byte)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(byte));
            byte expected = Convert.ToByte(s);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("256")]
        [TestCase("-1")]
        public void TestOverflowByte(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(byte)));
        }

        [Test]
        [TestCase("32768")]
        [TestCase("-32769")]
        public void TestOverflowInt16(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int16)));
        }

        [Test]
        [TestCase("2147483648")]
        [TestCase("-2147483649")]
        public void TestOverflowInt32(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int32)));
        }

        [Test]
        [TestCase("9223372036854775808")]
        [TestCase("-9223372036854775809")]
        public void TestOverflowInt64(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int64)));
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
            decimal actual = (decimal)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(decimal));
            decimal expected = Convert.ToDecimal(s, CultureInfo.InvariantCulture);

            Assert.AreEqual(expected, actual);
        }

        [Test]
        [TestCase("79228162514264337593543950336")] // Max decimal value + 1
        [TestCase("-79228162514264337593543950336")] // Min decimal value - 1
        [TestCase("79228162514264337593543950335.9999999999999999999999999999")] // The scaling factor range is 0 to 28. Scaling factor = 29 and fractional part > MaxValue
        public void TestOverflowDecimal(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(decimal)));
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
            double actualDouble = (double)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(double));
            double expectedDoulbe = Convert.ToDouble(s, CultureInfo.InvariantCulture);

            Assert.AreEqual(actualDouble, expectedDoulbe);

            float actualFloat = (float)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(float));
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
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int32)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int64)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int16)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(byte)));
        }

        [Test]
        [TestCase("thisIsNotAValidValue")]
        public void TestInvalidConversionInvalidFloat(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(float)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(double)));
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
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(decimal)));
        }

        private string DateTimeToLtzWireFormat(DateTime utcDateTime)
        {
            var tickDiff = utcDateTime.Ticks - SFDataConverter.UnixEpoch.Ticks;
            return (tickDiff / 10000000.0m).ToString(CultureInfo.InvariantCulture);
        }

        [Test]
        public void TestConvertTimestampLtzToDateTimeOffsetWithLocalTimezone()
        {
            var utcDateTime = new DateTime(2024, 7, 15, 10, 30, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), TimeZoneInfo.Local);

            var expected = new DateTimeOffset(utcDateTime).ToLocalTime();
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestConvertTimestampLtzToDateTimeOffsetWithNamedTimezone()
        {
            var tokyoTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Asia/Tokyo");
            var utcDateTime = new DateTime(2024, 7, 15, 10, 30, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), tokyoTz);

            Assert.AreEqual(TimeSpan.FromHours(9), result.Offset);
            Assert.AreEqual(utcDateTime, result.UtcDateTime);
        }

        [Test]
        public void TestConvertTimestampLtzToDateTimeWithLocalTimezone()
        {
            var utcDateTime = new DateTime(2024, 1, 15, 18, 0, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTime), TimeZoneInfo.Local);

            var expected = new DateTimeOffset(utcDateTime).ToLocalTime().DateTime;
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestConvertTimestampLtzToDateTimeWithNamedTimezone()
        {
            var warsawTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Europe/Warsaw");
            var utcDateTime = new DateTime(2024, 7, 15, 10, 30, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTime), warsawTz);

            var expected = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, warsawTz);
            Assert.AreEqual(expected, result);
            Assert.AreEqual(DateTimeKind.Local, result.Kind);
        }

        [Test]
        public void TestConvertTimestampLtzToDateTimeOffsetWithUtcTimezone()
        {
            var utcDateTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), TimeZoneInfo.Utc);

            Assert.AreEqual(TimeSpan.Zero, result.Offset);
            Assert.AreEqual(utcDateTime, result.UtcDateTime);
        }

        [Test]
        [TestCase("1969-12-31 23:59:59.1234567")]
        [TestCase("1960-06-15 10:30:45.5000000")]
        [TestCase("1900-01-01 00:00:00.0000001")]
        [TestCase("1969-12-31 23:59:59.9999999")]
        public void TestConvertTimestampLtzPreEpochWithFractionalSeconds(string inputTimeStr)
        {
            var utcDateTime = DateTime.SpecifyKind(
                DateTime.ParseExact(inputTimeStr, "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
                DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), TimeZoneInfo.Utc);

            Assert.AreEqual(utcDateTime, result.UtcDateTime);
        }

        [Test]
        [TestCase("-0.876543300", "1969-12-31 23:59:59.1234567")]
        [TestCase("-1.500000000", "1969-12-31 23:59:58.5000000")]
        [TestCase("-0.000000100", "1969-12-31 23:59:59.9999999")]
        public void TestConvertTimestampNtzPreEpochWithNegativeWireValue(string wireValue, string expectedStr)
        {
            var expected = DateTime.ParseExact(expectedStr, "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);

            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_NTZ, typeof(DateTime));

            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestConvertTimestampLtzThrowsWhenSessionTimezoneIsNull()
        {
            var wireValue = DateTimeToLtzWireFormat(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));

            Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(
                    ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), null));

            Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(
                    ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTime), null));
        }

        [Test]
        [TestCase(SFDataType.TIMESTAMP_LTZ, typeof(DateTime))]
        [TestCase(SFDataType.TIMESTAMP_TZ, typeof(DateTime))]
        [TestCase(SFDataType.TIMESTAMP_NTZ, typeof(DateTimeOffset))]
        [TestCase(SFDataType.TIME, typeof(DateTimeOffset))]
        [TestCase(SFDataType.DATE, typeof(DateTimeOffset))]
        public void TestInvalidTimestampConversion(SFDataType dataType, Type unsupportedType)
        {
            object unsupportedObject;
            if (unsupportedType == typeof(DateTimeOffset))
                unsupportedObject = new DateTimeOffset();
            else if (unsupportedType == typeof(DateTime))
                unsupportedObject = new DateTime();
            else
                unsupportedObject = null;

            Assert.NotNull(unsupportedObject);
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => SFDataConverter.CSharpValToSfVal(dataType, unsupportedObject));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        [TestCase(DbType.AnsiStringFixedLength, "hello", "hello")]
        [TestCase(DbType.AnsiString, "hello", "hello")]
        [TestCase(DbType.String, "hello", "hello")]
        [TestCase(DbType.StringFixedLength, "hello", "hello")]
        public void TestCSharpTypeValToSfTypeValTextTypes(DbType dbType, string srcVal, string expectedVal)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, srcVal);
            Assert.AreEqual("TEXT", result.Item1);
            Assert.AreEqual(expectedVal, result.Item2);
        }

        [Test]
        public void TestCSharpTypeValToSfTypeValGuidMapsToText()
        {
            var guid = new Guid("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Guid, guid);
            Assert.AreEqual("TEXT", result.Item1);
            Assert.AreEqual("a1b2c3d4-e5f6-7890-abcd-ef1234567890", result.Item2);
        }

        [Test]
        [TestCase(DbType.Decimal, 42.4f, "42.4")]
        [TestCase(DbType.Decimal, 42.3d, "42.3")]
        [TestCase(DbType.SByte, -1, "-1")]
        [TestCase(DbType.Int16, 123, "123")]
        [TestCase(DbType.Int32, 42, "42")]
        [TestCase(DbType.Int64, 9999999999L, "9999999999")]
        [TestCase(DbType.Byte, 255, "255")]
        [TestCase(DbType.UInt16, 65535u, "65535")]
        [TestCase(DbType.UInt32, 4294967295u, "4294967295")]
        [TestCase(DbType.UInt64, 18446744073709551615ul, "18446744073709551615")]
        [TestCase(DbType.VarNumeric, 123, "123")]
        public void TestCSharpTypeValToSfTypeValNumericTypes(DbType dbType, object srcVal, string expectedVal)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, srcVal);
            Assert.AreEqual("FIXED", result.Item1);
            Assert.AreEqual(expectedVal, result.Item2);
        }

        [Test]
        [TestCase(true, "True")]
        [TestCase(false, "False")]
        public void TestCSharpTypeValToSfTypeValBoolean(bool srcVal, string expectedVal)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Boolean, srcVal);
            Assert.AreEqual("BOOLEAN", result.Item1);
            Assert.AreEqual(expectedVal, result.Item2);
        }

        [Test]
        [TestCase(DbType.Double, 1.5d, "REAL", "1.5")]
        [TestCase(DbType.Single, 1.5f, "REAL", "1.5")]
        public void TestCSharpTypeValToSfTypeValRealTypes(DbType dbType, object srcVal, string expectedType, string expectedValue)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, srcVal);
            Assert.AreEqual(expectedType, result.Item1);
            Assert.AreEqual(expectedValue, result.Item2);
        }

        [Test]
        public void TestCSharpTypeValToSfTypeValTime()
        {
            var dt = new DateTime(2024, 1, 1, 13, 45, 30, 500);
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Time, dt);
            Assert.AreEqual("TIME", result.Item1);
            Assert.AreEqual("49530500000000", result.Item2);
        }

        [Test]
        [TestCase(DbType.DateTime)]
        [TestCase(DbType.DateTime2)]
        public void TestCSharpTypeValToSfTypeValTimestampNtz(DbType dbType)
        {
            var dt = new DateTime(2024, 7, 15, 10, 30, 0);
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, dt);
            Assert.AreEqual("TIMESTAMP_NTZ", result.Item1);
            Assert.AreEqual("1721039400000000000", result.Item2);
        }

        [Test]
        public void TestCSharpTypeValToSfTypeValTimestampTz()
        {
            var dto = new DateTimeOffset(2024, 7, 15, 10, 30, 0, TimeSpan.FromHours(5));
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.DateTimeOffset, dto);
            Assert.AreEqual("TIMESTAMP_TZ", result.Item1);
            Assert.AreEqual("1721021400000000000 1740", result.Item2);
        }

        [Test]
        public void TestCSharpTypeValToSfTypeValBinary()
        {
            var bytes = new byte[] { 0xCA, 0xFE };
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Binary, bytes);
            Assert.AreEqual("BINARY", result.Item1);
            Assert.AreEqual("cafe", result.Item2);
        }

        [Test]
        public void TestCSharpTypeValToSfTypeValUnsupportedTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Currency, 1.0m));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.UNSUPPORTED_DOTNET_TYPE);
        }

        [Test]
        public void TestConvertToCSharpValNullReturnsDbNull()
        {
            var result = SFDataConverter.ConvertToCSharpVal(null, SFDataType.TEXT, typeof(string));
            Assert.AreEqual(DBNull.Value, result);
        }

        [Test]
        public void TestConvertToCSharpValString()
        {
            var result = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("hello world"), SFDataType.TEXT, typeof(string));
            Assert.AreEqual("hello world", result);
        }

        [Test]
        public void TestConvertToCSharpValGuid()
        {
            var expected = new Guid("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
            var result = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("a1b2c3d4-e5f6-7890-abcd-ef1234567890"), SFDataType.TEXT, typeof(Guid));
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestConvertToCSharpValByteArrayFromBinary()
        {
            var expected = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE };
            var hex = "cafebabe";
            var result = (byte[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(hex), SFDataType.BINARY, typeof(byte[]));
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestConvertToCSharpValByteArrayFromNonBinary()
        {
            var input = "hello";
            var expected = Encoding.UTF8.GetBytes(input);
            var result = (byte[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(input), SFDataType.TEXT, typeof(byte[]));
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestConvertToCSharpValCharArrayFromBinary()
        {
            var hex = "68656c6c6f"; // "hello" in hex
            var result = (char[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(hex), SFDataType.BINARY, typeof(char[]));
            Assert.AreEqual("hello".ToCharArray(), result);
        }

        [Test]
        public void TestConvertToCSharpValCharArrayFromNonBinary()
        {
            var input = "hello";
            var result = (char[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(input), SFDataType.TEXT, typeof(char[]));
            Assert.AreEqual("hello".ToCharArray(), result);
        }

        [Test]
        public void TestConvertToCSharpValDateTimeFromDate()
        {
            // DATE type: value is days since Unix epoch
            var daysStr = "19723"; // 2024-01-01 = 19723 days since 1970-01-01
            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(daysStr), SFDataType.DATE, typeof(DateTime));
            Assert.AreEqual(new DateTime(2024, 1, 1), result);
            Assert.AreEqual(DateTimeKind.Unspecified, result.Kind);
        }

        [Test]
        public void TestConvertToCSharpValInvalidDestTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("1"), SFDataType.FIXED, typeof(uint)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INTERNAL_ERROR);
        }

        [Test]
        public void TestConvertToCSharpValTimeSpanWithNonTimeTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.FIXED, typeof(TimeSpan)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestConvertToCSharpValDateTimeWithUnsupportedSrcTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.FIXED, typeof(DateTime)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestConvertToCSharpValDateTimeOffsetWithUnsupportedSrcTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.FIXED, typeof(DateTimeOffset)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestConvertToCSharpValTimestampTzMissingSpaceThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.TIMESTAMP_TZ, typeof(DateTimeOffset)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INTERNAL_ERROR);
        }

        [Test]
        public void TestCSharpValToSfValNullReturnsNull()
        {
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TEXT, null);
            Assert.IsNull(result);
        }

        [Test]
        public void TestCSharpValToSfValDbNullReturnsNull()
        {
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TEXT, DBNull.Value);
            Assert.IsNull(result);
        }

        [TestCase(13, 45, 30, 500, "49530500000000")]
        [TestCase(23, 59, 59, 999, "86399999000000")]
        public void TestCSharpValToSfValTime(int hour, int minute, int second, int millisecond, string expected)
        {
            var dt = new DateTime(2024, 1, 1, hour, minute, second, millisecond);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIME, dt);
            Assert.AreEqual(expected, result);
        }

        [TestCase(13, 45, 30, 500, "1704116730500000000")]
        [TestCase(23, 59, 59, 999, "1704153599999000000")]
        public void TestCSharpValToSfValTimestampNtz(int hour, int minute, int second, int millisecond, string expected)
        {
            var dt = new DateTime(2024, 1, 1, hour, minute, second, millisecond);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_NTZ, dt);
            Assert.AreEqual(expected, result);
        }

        [TestCase(13, 45, 30, 500, "1721051130500000000")]
        [TestCase(23, 59, 59, 999, "1721087999999000000")]
        public void TestCSharpValToSfValTimestampLtz(int hour, int minute, int second, int millisecond, string expected)
        {
            var dto = new DateTimeOffset(2024, 7, 15, hour, minute, second, millisecond, TimeSpan.Zero);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_LTZ, dto);
            Assert.AreEqual(expected, result);
        }

        [TestCase(13, 45, 30, 500, "1721033130500000000 1740")]
        [TestCase(23, 59, 59, 999, "1721069999999000000 1740")]
        public void TestCSharpValToSfValTimestampTz(int hour, int minute, int second, int millisecond, string expected)
        {
            var dto = new DateTimeOffset(2024, 7, 15, hour, minute, second, millisecond, TimeSpan.FromHours(5));
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_TZ, dto);
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void TestCSharpValToSfValBinary()
        {
            var bytes = new byte[] { 0xCA, 0xFE };
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.BINARY, bytes);
            Assert.AreEqual("cafe", result);
        }

        [Test]
        public void TestCSharpValToSfValBinaryWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.BINARY, "not bytes"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestCSharpValToSfValUnsupportedSfTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.VARIANT, "data"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM);
        }

        [Test]
        public void TestCSharpValToSfValTimeWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIME, "not a datetime"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestCSharpValToSfValDateWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.DATE, "not a datetime"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestCSharpValToSfValTimestampNtzWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_NTZ, "not a datetime"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestCSharpValToSfValTimestampLtzWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_LTZ, "not a datetimeoffset"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestCSharpValToSfValTimestampTzWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_TZ, "not a datetimeoffset"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [Test]
        public void TestCSharpValToSfValTimeMidnight()
        {
            var dt = new DateTime(2024, 1, 1, 0, 0, 0);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIME, dt);
            Assert.AreEqual("0", result);
        }
    }
}
