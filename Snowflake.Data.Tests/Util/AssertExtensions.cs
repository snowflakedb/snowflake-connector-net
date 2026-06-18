using System;
using System.Text;
using Xunit;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util
{
    public static class AssertExtensions
    {
        public static void NotEmptyString(string actual)
        {
            Assert.False(string.IsNullOrEmpty(actual));
        }

        public static void Equal<T>(T expected, T actual, string message)
        {
            if (expected.Equals(actual))
                return;

            throw new XunitException($"Expected {expected}, actual: {actual} \n" + message);
        }

        public static void AnySucceeds(params Action[] assert)
        {
            var failedAssertsMessages = new StringBuilder();
            foreach (var action in assert)
            {
                try
                {
                    action();
                    return;
                }
                catch (XunitException ex)
                {
                    failedAssertsMessages.AppendLine(ex.Message);
                }
            }

            throw new XunitException(failedAssertsMessages.ToString());
        }
    }
}
