using System;
using System.Data;
using System.Text;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Snowflake.Data.Core;
    using Xunit;
    using System.Threading;
    using System.Globalization;


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

        [SFFact]
        public void TestConvertBindToSFValFinlandLocale()
        {
            Thread testThread = new Thread(() =>
            {
                CultureInfo ci = new CultureInfo("en-FI");

                Thread.CurrentThread.CurrentCulture = ci;

                System.Tuple<string, string> t =
                    SFDataConverter.CSharpTypeValToSfTypeVal(System.Data.DbType.Double, 1.2345);

                Assert.Equal("REAL", t.Item1);
                Assert.Equal("1.2345", t.Item2);
            });
            testThread.Start();
            testThread.Join();
        }

        [SFTheory]
        [InlineData("0", false)]
        [InlineData("t", true)]
        [InlineData("T", true)]
        [InlineData("1", true)]
        [InlineData("anything else", false)]
        public void TestConvertBoolean(string inputBooleanString, bool expected)
        {
            var actual = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(inputBooleanString), SFDataType.BOOLEAN, typeof(bool));
            Assert.Equal(expected, actual);
        }

        [SFTheory]
        [InlineData("2100-12-31 23:59:59.9999999")]
        [InlineData("2200-01-01 11:22:33.4455667")]
        [InlineData("9999-12-31 23:59:59.9999999")]
        [InlineData("1982-01-18 16:20:00.6666666")]
        [InlineData(null)]
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
            Assert.Equal(inputTime, result);
        }

        [SFTheory]
        [InlineData("11:22:33.4455667")]
        [InlineData("23:59:59.9999999")]
        [InlineData("16:20:00.6666666")]
        [InlineData("00:00:00.0000000")]
        [InlineData("00:00:00")]
        [InlineData("23:59:59.1")]
        [InlineData("23:59:59.12")]
        [InlineData("23:59:59.123")]
        [InlineData("23:59:59.1234")]
        [InlineData("23:59:59.12345")]
        [InlineData("23:59:59.123456")]
        [InlineData("23:59:59.1234567")]
        [InlineData("23:59:59.12345678")]
        [InlineData("23:59:59.123456789")]
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
            Assert.Equal(expected, result);
        }

        [SFTheory]
        [InlineData("2100-12-31 23:59:59.9999999", DateTimeKind.Utc)]
        [InlineData("2100-12-31 23:59:59.9999999", DateTimeKind.Local)]
        [InlineData("2100-12-31 23:59:59.9999999", DateTimeKind.Unspecified)]
        [InlineData("2200-01-01 00:00:00.0000000", DateTimeKind.Utc)]
        [InlineData("2200-01-01 00:00:00.0000000", DateTimeKind.Local)]
        [InlineData("2200-01-01 00:00:00.0000000", DateTimeKind.Unspecified)]
        [InlineData("1960-01-01 00:00:00.0000000", DateTimeKind.Unspecified)]
        [InlineData("9999-12-31 23:59:59.9999999", DateTimeKind.Unspecified)]
        [InlineData("1982-01-18 16:20:00.6666666", DateTimeKind.Unspecified)]
        [InlineData("1982-01-18 23:59:59.0000000", DateTimeKind.Unspecified)]
        [InlineData(null, DateTimeKind.Unspecified)]
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
            Assert.Equal(dtExpected, dtResult);
        }

        [SFTheory]
        [InlineData("9223372036854775807")]
        [InlineData("-9223372036854775808")]
        [InlineData("-1")]
        [InlineData("999999999999999999")]
        public void TestConvertToInt64(string s)
        {
            Int64 actual = (Int64)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int64));
            Int64 expected = Convert.ToInt64(s);
            Assert.Equal(expected, actual);
        }

        [SFTheory]
        [InlineData("2147483647")]
        [InlineData("-2147483648")]
        [InlineData("-1")]
        [InlineData("0")]
        public void TestConvertToInt32(string s)
        {
            Int32 actual = (Int32)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int32));
            Int32 expected = Convert.ToInt32(s);
            Assert.Equal(expected, actual);
        }

        [SFTheory]
        [InlineData("32767")]
        [InlineData("-32768")]
        [InlineData("-1")]
        [InlineData("0")]
        public void TestConvertToInt16(string s)
        {
            Int16 actual = (Int16)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int16));
            Int16 expected = Convert.ToInt16(s);
            Assert.Equal(expected, actual);
        }

        [SFTheory]
        [InlineData("255")]
        [InlineData("0")]
        public void TestConvertToByte(string s)
        {
            byte actual = (byte)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(byte));
            byte expected = Convert.ToByte(s);
            Assert.Equal(expected, actual);
        }

        [SFTheory]
        [InlineData("256")]
        [InlineData("-1")]
        public void TestOverflowByte(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(byte)));
        }

        [SFTheory]
        [InlineData("32768")]
        [InlineData("-32769")]
        public void TestOverflowInt16(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int16)));
        }

        [SFTheory]
        [InlineData("2147483648")]
        [InlineData("-2147483649")]
        public void TestOverflowInt32(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int32)));
        }

        [SFTheory]
        [InlineData("9223372036854775808")]
        [InlineData("-9223372036854775809")]
        public void TestOverflowInt64(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int64)));
        }

        [SFTheory]
        [InlineData("9223372036854775807.9223372036854775807")]
        [InlineData("-9223372036854775807.1234567890")]
        [InlineData("-1.300")]
        [InlineData("999999999999999999.000000000000100000000000")]
        [InlineData("4294967295.4294967296")]
        [InlineData("-0.999")]
        [InlineData("307.48100000000000000000")]
        [InlineData("79228162514264337593543950335")] // Max decimal value
        [InlineData("-79228162514264337593543950335")] // Min decimal value
        [InlineData("9.9999999999999999999999999999")] // The scaling factor range is 0 to 28
        [InlineData("-9.9999999999999999999999999999")] // The scaling factor range is 0 to 28
        [InlineData("79228162514264337593543950334.9999999999999999999999999999")] //A Decimal object has 29 digits of precision. If s represents a number that has more than 29 digits, but has a fractional part and is within the range of MaxValue and MinValue, the number is rounded
        public void TestConvertToDecimal(string s)
        {
            decimal actual = (decimal)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(decimal));
            decimal expected = Convert.ToDecimal(s, CultureInfo.InvariantCulture);

            Assert.Equal(expected, actual);
        }

        [SFTheory]
        [InlineData("79228162514264337593543950336")] // Max decimal value + 1
        [InlineData("-79228162514264337593543950336")] // Min decimal value - 1
        [InlineData("79228162514264337593543950335.9999999999999999999999999999")] // The scaling factor range is 0 to 28. Scaling factor = 29 and fractional part > MaxValue
        public void TestOverflowDecimal(string s)
        {
            Assert.Throws<OverflowException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(decimal)));
        }

        [SFTheory]
        [InlineData("9223372036854775807.9223372036854775807")]
        [InlineData("-9223372036854775807.1234567890")]
        [InlineData("-1.300")]
        [InlineData("-0.999")]
        [InlineData("999999999999999999.000000000000100000000000")]
        [InlineData("4294967295.4294967296")]
        [InlineData("1.5e-36")]
        [InlineData("1.5e+38")]
        //[InlineData("inf")] -- TODO - Not supported yet
        //[InlineData("-inf")] -- TODO - Not supported yet
        [InlineData("NaN")]
        public void TestConvertToFloat(string s)
        {
            double actualDouble = (double)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(double));
            double expectedDoulbe = Convert.ToDouble(s, CultureInfo.InvariantCulture);

            Assert.Equal(actualDouble, expectedDoulbe);

            float actualFloat = (float)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(float));
            float expectedFloat = Convert.ToSingle(s, CultureInfo.InvariantCulture);

            Assert.Equal(expectedFloat, actualFloat);
        }

        [SFTheory]
        [InlineData("thisIsNotAValidValue")]
        [InlineData("-0.999")]
        [InlineData("-1.300")]
        [InlineData("425.426")]
        [InlineData("1.5e-36")]
        [InlineData("1.5e+38")]
        [InlineData("inf")]
        [InlineData("-inf")]
        [InlineData("NaN")]
        public void TestInvalidConversionInvalidInt(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int32)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int64)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(Int16)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(byte)));
        }

        [SFTheory]
        [InlineData("thisIsNotAValidValue")]
        public void TestInvalidConversionInvalidFloat(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(float)));
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(double)));
        }

        [SFTheory]
        [InlineData("thisIsNotAValidValue")]
        [InlineData("1.5e-36")]
        [InlineData("1.5e+38")]
        [InlineData("inf")]
        [InlineData("-inf")]
        [InlineData("NaN")]
        public void TestInvalidConversionInvalidDecimal(string s)
        {
            Assert.Throws<FormatException>(() => SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(s), SFDataType.FIXED, typeof(decimal)));
        }

        private string DateTimeToLtzWireFormat(DateTime utcDateTime)
        {
            var tickDiff = utcDateTime.Ticks - SFDataConverter.UnixEpoch.Ticks;
            return (tickDiff / 10000000.0m).ToString(CultureInfo.InvariantCulture);
        }

        [SFFact]
        public void TestConvertTimestampLtzToDateTimeOffsetWithLocalTimezone()
        {
            var utcDateTime = new DateTime(2024, 7, 15, 10, 30, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), TimeZoneInfo.Local);

            var expected = new DateTimeOffset(utcDateTime).ToLocalTime();
            Assert.Equal(expected, result);
        }

        [SFFact]
        public void TestConvertTimestampLtzToDateTimeOffsetWithNamedTimezone()
        {
            var tokyoTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Asia/Tokyo");
            var utcDateTime = new DateTime(2024, 7, 15, 10, 30, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), tokyoTz);

            Assert.Equal(TimeSpan.FromHours(9), result.Offset);
            Assert.Equal(utcDateTime, result.UtcDateTime);
        }

        [SFFact]
        public void TestConvertTimestampLtzToDateTimeWithLocalTimezone()
        {
            var utcDateTime = new DateTime(2024, 1, 15, 18, 0, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTime), TimeZoneInfo.Local);

            var expected = new DateTimeOffset(utcDateTime).ToLocalTime().DateTime;
            Assert.Equal(expected, result);
        }

        [SFFact]
        public void TestConvertTimestampLtzToDateTimeWithNamedTimezone()
        {
            var warsawTz = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Europe/Warsaw");
            var utcDateTime = new DateTime(2024, 7, 15, 10, 30, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTime), warsawTz);

            var expected = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, warsawTz);
            Assert.Equal(expected, result);
            Assert.Equal(DateTimeKind.Local, result.Kind);
        }

        [SFFact]
        public void TestConvertTimestampLtzToDateTimeOffsetWithUtcTimezone()
        {
            var utcDateTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var wireValue = DateTimeToLtzWireFormat(utcDateTime);

            var result = (DateTimeOffset)SFDataConverter.ConvertToCSharpVal(
                ConvertToUTF8Buffer(wireValue), SFDataType.TIMESTAMP_LTZ, typeof(DateTimeOffset), TimeZoneInfo.Utc);

            Assert.Equal(TimeSpan.Zero, result.Offset);
            Assert.Equal(utcDateTime, result.UtcDateTime);
        }

        [SFFact]
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

        [SFTheory]
        [InlineData(SFDataType.TIMESTAMP_LTZ, typeof(DateTime))]
        [InlineData(SFDataType.TIMESTAMP_TZ, typeof(DateTime))]
        [InlineData(SFDataType.TIMESTAMP_NTZ, typeof(DateTimeOffset))]
        [InlineData(SFDataType.TIME, typeof(DateTimeOffset))]
        [InlineData(SFDataType.DATE, typeof(DateTimeOffset))]
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

        [SFTheory]
        [InlineData(DbType.AnsiStringFixedLength, "hello", "hello")]
        [InlineData(DbType.AnsiString, "hello", "hello")]
        [InlineData(DbType.String, "hello", "hello")]
        [InlineData(DbType.StringFixedLength, "hello", "hello")]
        public void TestCSharpTypeValToSfTypeValTextTypes(DbType dbType, string srcVal, string expectedVal)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, srcVal);
            Assert.Equal("TEXT", result.Item1);
            Assert.Equal(expectedVal, result.Item2);
        }

        [SFFact]
        public void TestCSharpTypeValToSfTypeValGuidMapsToText()
        {
            var guid = new Guid("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Guid, guid);
            Assert.Equal("TEXT", result.Item1);
            Assert.Equal("a1b2c3d4-e5f6-7890-abcd-ef1234567890", result.Item2);
        }

        [SFTheory]
        [InlineData(DbType.Decimal, 42.4f, "42.4")]
        [InlineData(DbType.Decimal, 42.3d, "42.3")]
        [InlineData(DbType.SByte, -1, "-1")]
        [InlineData(DbType.Int16, 123, "123")]
        [InlineData(DbType.Int32, 42, "42")]
        [InlineData(DbType.Int64, 9999999999L, "9999999999")]
        [InlineData(DbType.Byte, 255, "255")]
        [InlineData(DbType.UInt16, 65535u, "65535")]
        [InlineData(DbType.UInt32, 4294967295u, "4294967295")]
        [InlineData(DbType.UInt64, 18446744073709551615ul, "18446744073709551615")]
        [InlineData(DbType.VarNumeric, 123, "123")]
        public void TestCSharpTypeValToSfTypeValNumericTypes(DbType dbType, object srcVal, string expectedVal)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, srcVal);
            Assert.Equal("FIXED", result.Item1);
            Assert.Equal(expectedVal, result.Item2);
        }

        [SFTheory]
        [InlineData(true, "True")]
        [InlineData(false, "False")]
        public void TestCSharpTypeValToSfTypeValBoolean(bool srcVal, string expectedVal)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Boolean, srcVal);
            Assert.Equal("BOOLEAN", result.Item1);
            Assert.Equal(expectedVal, result.Item2);
        }

        [SFTheory]
        [InlineData(DbType.Double, 1.5d, "REAL", "1.5")]
        [InlineData(DbType.Single, 1.5f, "REAL", "1.5")]
        public void TestCSharpTypeValToSfTypeValRealTypes(DbType dbType, object srcVal, string expectedType, string expectedValue)
        {
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, srcVal);
            Assert.Equal(expectedType, result.Item1);
            Assert.Equal(expectedValue, result.Item2);
        }

        [SFFact]
        public void TestCSharpTypeValToSfTypeValTime()
        {
            var dt = new DateTime(2024, 1, 1, 13, 45, 30, 500);
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Time, dt);
            Assert.Equal("TIME", result.Item1);
            Assert.Equal("49530500000000", result.Item2);
        }

        [SFTheory]
        [InlineData(DbType.DateTime)]
        [InlineData(DbType.DateTime2)]
        public void TestCSharpTypeValToSfTypeValTimestampNtz(DbType dbType)
        {
            var dt = new DateTime(2024, 7, 15, 10, 30, 0);
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(dbType, dt);
            Assert.Equal("TIMESTAMP_NTZ", result.Item1);
            Assert.Equal("1721039400000000000", result.Item2);
        }

        [SFFact]
        public void TestCSharpTypeValToSfTypeValTimestampTz()
        {
            var dto = new DateTimeOffset(2024, 7, 15, 10, 30, 0, TimeSpan.FromHours(5));
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.DateTimeOffset, dto);
            Assert.Equal("TIMESTAMP_TZ", result.Item1);
            Assert.Equal("1721021400000000000 1740", result.Item2);
        }

        [SFFact]
        public void TestCSharpTypeValToSfTypeValBinary()
        {
            var bytes = new byte[] { 0xCA, 0xFE };
            var result = SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Binary, bytes);
            Assert.Equal("BINARY", result.Item1);
            Assert.Equal("cafe", result.Item2);
        }

        [SFFact]
        public void TestCSharpTypeValToSfTypeValUnsupportedTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpTypeValToSfTypeVal(DbType.Currency, 1.0m));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.UNSUPPORTED_DOTNET_TYPE);
        }

        [SFFact]
        public void TestConvertToCSharpValNullReturnsDbNull()
        {
            var result = SFDataConverter.ConvertToCSharpVal(null, SFDataType.TEXT, typeof(string));
            Assert.Equal(DBNull.Value, result);
        }

        [SFFact]
        public void TestConvertToCSharpValString()
        {
            var result = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("hello world"), SFDataType.TEXT, typeof(string));
            Assert.Equal("hello world", result);
        }

        [SFFact]
        public void TestConvertToCSharpValGuid()
        {
            var expected = new Guid("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
            var result = SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("a1b2c3d4-e5f6-7890-abcd-ef1234567890"), SFDataType.TEXT, typeof(Guid));
            Assert.Equal(expected, result);
        }

        [SFFact]
        public void TestConvertToCSharpValByteArrayFromBinary()
        {
            var expected = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE };
            var hex = "cafebabe";
            var result = (byte[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(hex), SFDataType.BINARY, typeof(byte[]));
            Assert.Equal(expected, result);
        }

        [SFFact]
        public void TestConvertToCSharpValByteArrayFromNonBinary()
        {
            var input = "hello";
            var expected = Encoding.UTF8.GetBytes(input);
            var result = (byte[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(input), SFDataType.TEXT, typeof(byte[]));
            Assert.Equal(expected, result);
        }

        [SFFact]
        public void TestConvertToCSharpValCharArrayFromBinary()
        {
            var hex = "68656c6c6f"; // "hello" in hex
            var result = (char[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(hex), SFDataType.BINARY, typeof(char[]));
            Assert.Equal("hello".ToCharArray(), result);
        }

        [SFFact]
        public void TestConvertToCSharpValCharArrayFromNonBinary()
        {
            var input = "hello";
            var result = (char[])SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(input), SFDataType.TEXT, typeof(char[]));
            Assert.Equal("hello".ToCharArray(), result);
        }

        [SFFact]
        public void TestConvertToCSharpValDateTimeFromDate()
        {
            // DATE type: value is days since Unix epoch
            var daysStr = "19723"; // 2024-01-01 = 19723 days since 1970-01-01
            var result = (DateTime)SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer(daysStr), SFDataType.DATE, typeof(DateTime));
            Assert.Equal(new DateTime(2024, 1, 1), result);
            Assert.Equal(DateTimeKind.Unspecified, result.Kind);
        }

        [SFFact]
        public void TestConvertToCSharpValInvalidDestTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("1"), SFDataType.FIXED, typeof(uint)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INTERNAL_ERROR);
        }

        [SFFact]
        public void TestConvertToCSharpValTimeSpanWithNonTimeTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.FIXED, typeof(TimeSpan)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestConvertToCSharpValDateTimeWithUnsupportedSrcTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.FIXED, typeof(DateTime)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestConvertToCSharpValDateTimeOffsetWithUnsupportedSrcTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.FIXED, typeof(DateTimeOffset)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestConvertToCSharpValTimestampTzMissingSpaceThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.ConvertToCSharpVal(ConvertToUTF8Buffer("12345"), SFDataType.TIMESTAMP_TZ, typeof(DateTimeOffset)));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INTERNAL_ERROR);
        }

        [SFFact]
        public void TestCSharpValToSfValNullReturnsNull()
        {
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TEXT, null);
            Assert.Null(result);
        }

        [SFFact]
        public void TestCSharpValToSfValDbNullReturnsNull()
        {
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TEXT, DBNull.Value);
            Assert.Null(result);
        }

        [InlineData(13, 45, 30, 500, "49530500000000")]
        [InlineData(23, 59, 59, 999, "86399999000000")]
        public void TestCSharpValToSfValTime(int hour, int minute, int second, int millisecond, string expected)
        {
            var dt = new DateTime(2024, 1, 1, hour, minute, second, millisecond);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIME, dt);
            Assert.Equal(expected, result);
        }

        [InlineData(13, 45, 30, 500, "1704116730500000000")]
        [InlineData(23, 59, 59, 999, "1704153599999000000")]
        public void TestCSharpValToSfValTimestampNtz(int hour, int minute, int second, int millisecond, string expected)
        {
            var dt = new DateTime(2024, 1, 1, hour, minute, second, millisecond);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_NTZ, dt);
            Assert.Equal(expected, result);
        }

        [InlineData(13, 45, 30, 500, "1721051130500000000")]
        [InlineData(23, 59, 59, 999, "1721087999999000000")]
        public void TestCSharpValToSfValTimestampLtz(int hour, int minute, int second, int millisecond, string expected)
        {
            var dto = new DateTimeOffset(2024, 7, 15, hour, minute, second, millisecond, TimeSpan.Zero);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_LTZ, dto);
            Assert.Equal(expected, result);
        }

        [InlineData(13, 45, 30, 500, "1721033130500000000 1740")]
        [InlineData(23, 59, 59, 999, "1721069999999000000 1740")]
        public void TestCSharpValToSfValTimestampTz(int hour, int minute, int second, int millisecond, string expected)
        {
            var dto = new DateTimeOffset(2024, 7, 15, hour, minute, second, millisecond, TimeSpan.FromHours(5));
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_TZ, dto);
            Assert.Equal(expected, result);
        }

        [SFFact]
        public void TestCSharpValToSfValBinary()
        {
            var bytes = new byte[] { 0xCA, 0xFE };
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.BINARY, bytes);
            Assert.Equal("cafe", result);
        }

        [SFFact]
        public void TestCSharpValToSfValBinaryWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.BINARY, "not bytes"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestCSharpValToSfValUnsupportedSfTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.VARIANT, "data"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM);
        }

        [SFFact]
        public void TestCSharpValToSfValTimeWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIME, "not a datetime"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestCSharpValToSfValDateWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.DATE, "not a datetime"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestCSharpValToSfValTimestampNtzWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_NTZ, "not a datetime"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestCSharpValToSfValTimestampLtzWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_LTZ, "not a datetimeoffset"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestCSharpValToSfValTimestampTzWrongTypeThrows()
        {
            var ex = Assert.Throws<SnowflakeDbException>(() =>
                SFDataConverter.CSharpValToSfVal(SFDataType.TIMESTAMP_TZ, "not a datetimeoffset"));
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.INVALID_DATA_CONVERSION);
        }

        [SFFact]
        public void TestCSharpValToSfValTimeMidnight()
        {
            var dt = new DateTime(2024, 1, 1, 0, 0, 0);
            var result = SFDataConverter.CSharpValToSfVal(SFDataType.TIME, dt);
            Assert.Equal("0", result);
        }
    }
}
