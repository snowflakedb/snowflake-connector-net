using Snowflake.Data.Core;
using Snowflake.Data.Client;
using NUnit.Framework;

namespace Snowflake.Data.Tests.Util
{
    public static class SnowflakeDbExceptionAssert
    {
        public static void HasErrorCode(SnowflakeDbException exception, SFError sfError)
        {
            Assert.AreEqual(exception.ErrorCode, sfError.GetAttribute<SFErrorAttr>().errorCode);
        }
    }
}
