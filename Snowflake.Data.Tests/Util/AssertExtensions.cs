using NUnit.Framework;

namespace Snowflake.Data.Tests.Util
{
    public static class AssertExtensions
    {
        public static void NotEmptyString(string actual)
        {
            Assert.IsFalse(string.IsNullOrEmpty(actual));
        }
    }
}
