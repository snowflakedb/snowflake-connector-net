using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core
{
    public class FastParser
    {

        public static Int64 FastParseInt64(byte[] s, int offset, int len)
        {
            Int64 result = 0;
            int i = offset;
            bool isMinus = false;
            if (len > 0 && s[i] == '-')
            {
                isMinus = true;
                i++;
            }
            int end = len + offset;
            for (; i < end; i++)
            {
                if ((UInt64)result > (0x7fffffffffffffff / 10))
                    throw new OverflowException();
                int c = s[i] - '0';
                if (c < 0 || c > 9)
                    throw new FormatException();
                result = result * 10 + c;
            }
            if (isMinus)
            {
                result = -result;
                if (result > 0)
                    throw new OverflowException();
            }
            else
            {
                if (result < 0)
                    throw new OverflowException();
            }
            return result;
        }

        public static Int32 FastParseInt32(byte[] s, int offset, int len)
        {
            Int32 result = 0;
            int i = offset;
            bool isMinus = false;
            if (len > 0 && s[i] == '-')
            {
                isMinus = true;
                i++;
            }
            int end = len + offset;
            for (; i < end; i++)
            {
                if ((UInt32)result > (0x7fffffff / 10))
                    throw new OverflowException();
                int c = s[i] - '0';
                if (c < 0 || c > 9)
                    throw new FormatException();
                result = result * 10 + c;
            }
            if (isMinus)
            {
                result = -result;
                if (result > 0)
                    throw new OverflowException();
            }
            else
            {
                if (result < 0)
                    throw new OverflowException();
            }
            return result;
        }

        public static decimal FastParseDecimal(byte[] s, int offset, int len)
        {
            // Find any decimal point
            // Parse integer part and decimal part as 64-bit numbers
            // Calculate decimal number to return
            int decimalPos = Array.IndexOf<byte>(s, (byte)'.', offset, len);
            if (decimalPos < 0)
            {
                // No decimal point found, just parse as integer
                Int64 i1 = FastParseInt64(s, offset, len);
                return (decimal)i1;
            }
            else
            {
                decimalPos -= offset;
                int decimalLen = len - decimalPos - 1;
                Int64 intPart = FastParseInt64(s, offset, decimalPos);
                Int64 decimalPart = FastParseInt64(s, offset + decimalPos + 1, decimalLen);
                bool isMinus = false;
                if (decimalPart < 0)
                    throw new FormatException();
                if (intPart < 0)
                {
                    isMinus = true;
                    intPart = -intPart;
                    if (intPart < 0)
                        throw new OverflowException();
                }
                decimal d1 = new decimal(intPart);
                decimal d2 = new decimal((int)(decimalPart & 0xffffffff), (int)((decimalPart >> 32) & 0xffffffff), 0, false, (byte)decimalLen);
                decimal result = d1 + d2;
                if (isMinus)
                    result = -result;
                return result;
            }
        }
    }
}
