/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Linq;

namespace Snowflake.Data.Tests.Util
{

    public class TestDataGenarator
    {
        private static Random random = new Random();
        private static string lowercaseChars = "abcdefghijklmnopqrstuvwxyz";
        private static string uppercaseChars = lowercaseChars.ToUpper();
        private static string nonZeroDigits = "123456789";
        private static string digitChars = "0" + nonZeroDigits;
        private static string letterChars = lowercaseChars + uppercaseChars;
        private static string alphanumericChars = letterChars + digitChars;
        
        public static bool NextBool()
        {
            return random.Next(0, 1) == 1;
        }

        public static int NextInt(int minValueInclusive, int maxValueExclusive)
        {
            return random.Next(minValueInclusive, maxValueExclusive);
        }
        
        public static string NextAlphaNumeric()
        {
            return NextLetter() +
                   Enumerable.Repeat(alphanumericChars, random.Next(5, 12))
                       .Select(NextChar)
                       .Aggregate((s1, s2) => s1 + s2);
        }

        public static string NextDigitsString(int length)
        {
            if (length == 1)
            {
                return NextNonZeroDigitString();
            } 
            return NextNonZeroDigitString() + Enumerable.Repeat(digitChars, length - 1)
                .Select(NextChar)
                .Aggregate((s1, s2) => s1 + s2);
        }

        public static string NextNonZeroDigitString()
        {
            return NextChar(nonZeroDigits);
        }

        private static string NextLetter()
        {
            return NextChar(letterChars);
        }

        private static string NextChar(string chars)
        {
            return chars[random.Next(chars.Length)].ToString();
        }
    }
}
