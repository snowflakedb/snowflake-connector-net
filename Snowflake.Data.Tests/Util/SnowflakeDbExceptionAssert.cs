using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using Snowflake.Data.Core;
using Snowflake.Data.Client;
using Xunit;

namespace Snowflake.Data.Tests.Util
{
    public static class SnowflakeDbExceptionAssert
    {
        public static void HasErrorCode(SnowflakeDbException exception, SFError sfError)
        {
            Assert.Equal(sfError.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
        }

        public static void HasErrorCode(Exception exception, SFError sfError)
        {
            Assert.NotNull(exception);
            switch (exception)
            {
                case SnowflakeDbException snowflakeDbException:
                    Assert.Equal(sfError.GetAttribute<SFErrorAttr>().errorCode, snowflakeDbException.ErrorCode);
                    break;
                default:
                    Assert.Fail(exception.GetType() + " type is not " + typeof(SnowflakeDbException));
                    break;
            }
        }

        public static void HasErrorCodeInExceptionChain(Exception exception, SFError sfError)
        {
            var exceptions = CollectExceptions(exception);
            var errorCodes = exceptions.OfType<SnowflakeDbException>().Select(x => x.ErrorCode).Distinct().ToArray();
            var expectedErrorCode = sfError.GetAttribute<SFErrorAttr>().errorCode;
            Assert.Contains(expectedErrorCode, errorCodes);
        }

        public static void HasHttpErrorCodeInExceptionChain(Exception exception, HttpStatusCode expected)
        {
            var exceptions = CollectExceptions(exception);
            Assert.True(exceptions.Any(e =>
                {
                    switch (e)
                    {
                        case SnowflakeDbException se:
                            return se.ErrorCode == (int)expected;
                        case HttpRequestException he:
                            return he.Message.Contains(((int)expected).ToString());
                        default:
                            return false;
                    }
                }));
        }

        public static void HasMessageInExceptionChain(Exception exception, string expected)
        {
            var exceptions = CollectExceptions(exception);
            Assert.True(exceptions.Any(e => e.Message.Contains(expected)));
        }

        private static List<Exception> CollectExceptions(Exception exception)
        {
            var collected = new List<Exception>();
            if (exception is null)
                return collected;
            switch (exception)
            {
                case AggregateException aggregate:
                    var inner = aggregate.Flatten().InnerExceptions;
                    collected.AddRange(inner);
                    collected.AddRange(inner
                        .Where(e => e.InnerException != null)
                        .SelectMany(e => CollectExceptions(e.InnerException)));
                    break;
                case Exception general:
                    collected.AddRange(CollectExceptions(general.InnerException));
                    collected.Add(general);
                    break;
            }
            return collected;
        }
    }
}
