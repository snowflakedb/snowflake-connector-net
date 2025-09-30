using System;
using System.Linq;

namespace Snowflake.Data.Tests.Util
{

    public class TestDataGenarator
    {
        private static Random s_random = new Random();
        private static string s_lowercaseChars = "abcdefghijklmnopqrstuvwxyz";
        private static string s_uppercaseChars = s_lowercaseChars.ToUpper();
        private static string s_nonZeroDigits = "123456789";
        private static string s_digitChars = "0" + s_nonZeroDigits;
        private static string s_letterChars = s_lowercaseChars + s_uppercaseChars;
        private static string s_alphanumericChars = s_letterChars + s_digitChars;

        public static string AsciiCodes => new String(Enumerable.Range(0, 256).Select(ch => (char)ch).ToArray());
        public static char SnowflakeUnicode => '\u2744';
        public static string EmojiUnicode => "\uD83D\uDE00";
        public static string StringWithUnicode => AsciiCodes + SnowflakeUnicode + EmojiUnicode;

        public static bool NextBool()
        {
            return s_random.Next(0, 1) == 1;
        }

        public static int NextInt(int minValueInclusive, int maxValueExclusive)
        {
            return s_random.Next(minValueInclusive, maxValueExclusive);
        }

        public static string NextAlphaNumeric()
        {
            return NextAlphaNumeric(s_random.Next(5, 12));
        }

        public static string NextAlphaNumeric(int length)
        {
            if (length < 1)
            {
                return "";
            }
            var buffer = new char[length];
            buffer[0] = NextLetterChar();
            for (var i = 1; i < length; i++)
            {
                buffer[i] = NextAlphaNumericChar();
            }
            return new string(buffer);
        }

        public static string NextDigitsString(int length)
        {
            if (length < 1)
            {
                return "";
            }

            if (length == 1)
            {
                NextDigitAsString();
            }
            var buffer = new char[length];
            buffer[0] = NextNonZeroDigitChar();
            for (var i = 1; i < length; i++)
            {
                buffer[i] = NextDigitChar();
            }
            return new string(buffer);
        }

        public static byte[] NextBytes(int length)
        {
            var buffer = new byte[length];
            s_random.NextBytes(buffer);
            return buffer;
        }

        public static T GetRandomEnumValue<T>()
        {
            var values = Enum.GetValues(typeof(T));
            if (values.Length == 0)
                throw new Exception("Could not get enum value because it doesn't have any value");
            if (values.Length == 1)
                return values.OfType<T>().First();
            var randomIndex = NextInt(0, values.Length);
            return values.OfType<T>().ElementAt(randomIndex);
        }

        private static char NextAlphaNumericChar() => NextChar(s_alphanumericChars);

        public static string NextNonZeroDigitAsString() => NextNonZeroDigitChar().ToString();

        private static char NextNonZeroDigitChar() => NextChar(s_nonZeroDigits);

        private static string NextDigitAsString() => NextDigitChar().ToString();

        private static char NextDigitChar() => NextChar(s_digitChars);

        private static string NextLetterAsString() => NextLetterChar().ToString();

        private static char NextLetterChar() => NextChar(s_letterChars);

        private static string NextCharAsString(string chars) => NextChar(chars).ToString();

        private static char NextChar(string chars) => chars[s_random.Next(chars.Length)];
    }
}
