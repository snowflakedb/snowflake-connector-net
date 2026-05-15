using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System;
    using System.Text;
    public sealed class FastParserTest : IDisposable
    {
        byte[] _byte;

        public void Dispose()
        {
            _byte = null;
        }

        [SFFact]
        public void TestFastParseInt64WithLongMaxValue()
        {
            long expectedLongValue = long.MaxValue;
            _byte = Encoding.UTF8.GetBytes(expectedLongValue.ToString());

            long actualLongValue = FastParser.FastParseInt64(_byte, 0, _byte.Length);
            Assert.Equal(expectedLongValue, actualLongValue);
        }

        [SFFact]
        public void TestFastParseInt64WithPositiveOverflow()
        {
            // Int64.MaxValue + 1
            string int64MaxValuePlusOne = "9223372036854775808";
            _byte = Encoding.UTF8.GetBytes(int64MaxValuePlusOne);

            Assert.Throws<OverflowException>(() => FastParser.FastParseInt64(_byte, 0, _byte.Length));
        }

        [SFFact]
        public void TestFastParseInt64WithNegativeOverflow()
        {
            // Int64.MinValue - 1
            string int64MinValueMinusOne = "-9223372036854775809";
            _byte = Encoding.UTF8.GetBytes(int64MinValueMinusOne);

            Assert.Throws<OverflowException>(() => FastParser.FastParseInt64(_byte, 0, _byte.Length));
        }

        [SFFact]
        public void TestFastParseInt64ThrowsWrongFormat()
        {
            Assert.Throws<FormatException>(() => FastParser.FastParseInt64(new byte[1], 0, 1));
        }

        [SFFact]
        public void TestFastParseInt32WithIntMaxValue()
        {
            int expectedIntValue = int.MaxValue;
            _byte = Encoding.UTF8.GetBytes(expectedIntValue.ToString());

            int actualIntValue = FastParser.FastParseInt32(_byte, 0, _byte.Length);
            Assert.Equal(expectedIntValue, actualIntValue);
        }

        [SFFact]
        public void TestFastParseInt32WithPositiveOverflow()
        {
            // Int32.MaxValue + 1
            string int32MaxValuePlusOne = "2147483648";
            _byte = Encoding.UTF8.GetBytes(int32MaxValuePlusOne);

            Assert.Throws<OverflowException>(() => FastParser.FastParseInt32(_byte, 0, _byte.Length));
        }

        [SFFact]
        public void TestFastParseInt32WithNegativeOverflow()
        {
            // Int32.MinValue - 1
            string int32MinValueMinusOne = "-2147483649";
            _byte = Encoding.UTF8.GetBytes(int32MinValueMinusOne);

            Assert.Throws<OverflowException>(() => FastParser.FastParseInt32(_byte, 0, _byte.Length));
        }

        [SFFact]
        public void TestFastParseInt32ThrowsWrongFormat()
        {
            Assert.Throws<FormatException>(() => FastParser.FastParseInt32(new byte[1], 0, 1));
        }

        [SFFact]
        public void TestFastParseDecimalWithLongMaxValuePlusOne()
        {
            // Int64.MaxValue + 1
            ulong int64MaxValuePlusOne = 9223372036854775808;
            _byte = Encoding.UTF8.GetBytes(int64MaxValuePlusOne.ToString());

            decimal actualDecimalValue = FastParser.FastParseDecimal(_byte, 0, _byte.Length);
            Assert.Equal(int64MaxValuePlusOne, actualDecimalValue);
        }

        [SFFact]
        public void TestFastParseDecimalWithLongMaxValuePlusDecimal()
        {
            // Int64.MaxValue + 1.123M
            decimal int64MaxValuePlusOneWithDecimal = 9223372036854775808.123M;
            string int64MaxValuePlusOneWithDecimalString = "9223372036854775808.123";
            _byte = Encoding.UTF8.GetBytes(int64MaxValuePlusOneWithDecimalString);

            decimal actualDecimalValue = FastParser.FastParseDecimal(_byte, 0, _byte.Length);
            Assert.Equal(int64MaxValuePlusOneWithDecimal, actualDecimalValue);
        }

        [SFFact]
        public void TestFastParseDecimalWithPositiveDecimal()
        {
            decimal expectedDecimalValue = 1.2345678M;
            _byte = Encoding.UTF8.GetBytes(expectedDecimalValue.ToString());

            decimal actualDecimalValue = FastParser.FastParseDecimal(_byte, 0, _byte.Length);
            Assert.Equal(expectedDecimalValue, actualDecimalValue);
        }

        [SFFact]
        public void TestFastParseDecimalWithNegativeDecimal()
        {
            decimal expectedDecimalValue = -1.2345678M;
            _byte = Encoding.UTF8.GetBytes(expectedDecimalValue.ToString());

            decimal actualDecimalValue = FastParser.FastParseDecimal(_byte, 0, _byte.Length);
            Assert.Equal(expectedDecimalValue, actualDecimalValue);
        }

        [SFFact]
        public void TestFastParseDecimalWithoutDecimalInTheValue()
        {
            decimal expectedDecimalValue = 12345678;
            _byte = Encoding.UTF8.GetBytes(expectedDecimalValue.ToString());

            decimal actualDecimalValue = FastParser.FastParseDecimal(_byte, 0, _byte.Length);
            Assert.Equal(expectedDecimalValue, actualDecimalValue);
        }

        [SFFact]
        public void TestFastParseDecimalWithNullByteArray()
        {
            UTF8Buffer srcVal = new UTF8Buffer(null, 0, 0);

            Exception ex = Assert.Throws<SnowflakeDbException>(() => FastParser.FastParseDecimal(srcVal.Buffer, srcVal.offset, srcVal.length));
            Assert.Matches(".*Cannot parse a null buffer.*", ex.Message);
        }
    }
}
