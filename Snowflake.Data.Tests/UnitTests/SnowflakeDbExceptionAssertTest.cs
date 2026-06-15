using System;
using System.Net;
using System.Net.Http;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class SnowflakeDbExceptionAssertTest
{
    [SFFact]
    public void TestHasErrorCode_MatchingCode_Passes()
    {
        // arrange
        var exception = new SnowflakeDbException(SFError.INTERNAL_ERROR, "test");

        // act & assert - should not throw
        SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.INTERNAL_ERROR);
    }

    [SFFact]
    public void TestHasErrorCode_MismatchedCode_Throws()
    {
        // arrange
        var exception = new SnowflakeDbException(SFError.INTERNAL_ERROR, "test");

        // act & assert
        Assert.ThrowsAny<XunitException>(() =>
            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.REQUEST_TIMEOUT));
    }

    [SFFact]
    public void TestHasErrorCode_ExceptionOverload_WithSnowflakeDbException_Passes()
    {
        // arrange
        Exception exception = new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY, "host");

        // act & assert - should not throw
        SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.MISSING_CONNECTION_PROPERTY);
    }

    [SFFact]
    public void TestHasErrorCode_ExceptionOverload_WithNonSnowflakeException_Throws()
    {
        // arrange
        Exception exception = new InvalidOperationException("not a snowflake exception");

        // act & assert
        Assert.ThrowsAny<XunitException>(() =>
            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.INTERNAL_ERROR));
    }

    [SFFact]
    public void TestHasErrorCode_ExceptionOverload_NullException_Throws()
    {
        // act & assert
        Assert.ThrowsAny<XunitException>(() =>
            SnowflakeDbExceptionAssert.HasErrorCode((Exception)null, SFError.INTERNAL_ERROR));
    }

    [SFFact]
    public void TestHasErrorCodeInExceptionChain_DirectException_Passes()
    {
        // arrange
        var exception = new SnowflakeDbException(SFError.REQUEST_TIMEOUT, "timed out");

        // act & assert
        SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(exception, SFError.REQUEST_TIMEOUT);
    }

    [SFFact]
    public void TestHasErrorCodeInExceptionChain_WrappedInAggregateException_Passes()
    {
        // arrange
        var inner = new SnowflakeDbException(SFError.QUERY_CANCELLED, "cancelled");
        var exception = new AggregateException(inner);

        // act & assert
        SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(exception, SFError.QUERY_CANCELLED);
    }

    [SFFact]
    public void TestHasErrorCodeInExceptionChain_NestedInInnerException_Passes()
    {
        // arrange
        var snowflakeEx = new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, "bad string");
        var wrapper = new InvalidOperationException("wrapper", snowflakeEx);

        // act & assert
        SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(wrapper, SFError.INVALID_CONNECTION_STRING);
    }

    [SFFact]
    public void TestHasErrorCodeInExceptionChain_NoMatchingCode_Throws()
    {
        // arrange
        var exception = new SnowflakeDbException(SFError.INTERNAL_ERROR, "error");

        // act & assert
        Assert.ThrowsAny<Exception>(() =>
            SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(exception, SFError.REQUEST_TIMEOUT));
    }

    [SFFact]
    public void TestHasErrorCodeInExceptionChain_DeeplyNestedInAggregate_Passes()
    {
        // arrange
        var snowflakeEx = new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE, "feature");
        var mid = new InvalidOperationException("mid", snowflakeEx);
        var exception = new AggregateException(mid);

        // act & assert
        SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(exception, SFError.UNSUPPORTED_FEATURE);
    }

    [SFFact]
    public void TestHasHttpErrorCodeInExceptionChain_MatchingSnowflakeException_Passes()
    {
        // arrange
        var exception = new SnowflakeDbException("sqlState", (int)HttpStatusCode.Forbidden, "forbidden", "queryId");

        // act & assert
        SnowflakeDbExceptionAssert.HasHttpErrorCodeInExceptionChain(exception, HttpStatusCode.Forbidden);
    }

    [SFFact]
    public void TestHasHttpErrorCodeInExceptionChain_MatchingHttpRequestException_Passes()
    {
        // arrange
        var httpEx = new HttpRequestException("Response status code does not indicate success: 403");
        var exception = new AggregateException(httpEx);

        // act & assert
        SnowflakeDbExceptionAssert.HasHttpErrorCodeInExceptionChain(exception, HttpStatusCode.Forbidden);
    }

    [SFFact]
    public void TestHasHttpErrorCodeInExceptionChain_NoMatch_Throws()
    {
        // arrange
        var exception = new InvalidOperationException("no http error here");

        // act & assert
        Assert.ThrowsAny<Exception>(() =>
            SnowflakeDbExceptionAssert.HasHttpErrorCodeInExceptionChain(exception, HttpStatusCode.NotFound));
    }

    [SFFact]
    public void TestHasMessageInExceptionChain_DirectMatch_Passes()
    {
        // arrange
        var exception = new InvalidOperationException("connection refused by server");

        // act & assert
        SnowflakeDbExceptionAssert.HasMessageInExceptionChain(exception, "connection refused");
    }

    [SFFact]
    public void TestHasMessageInExceptionChain_NestedMatch_Passes()
    {
        // arrange
        var inner = new TimeoutException("operation timed out after 30s");
        var exception = new AggregateException(inner);

        // act & assert
        SnowflakeDbExceptionAssert.HasMessageInExceptionChain(exception, "timed out");
    }

    [SFFact]
    public void TestHasMessageInExceptionChain_NoMatch_Throws()
    {
        // arrange
        var exception = new InvalidOperationException("something else happened");

        // act & assert
        Assert.ThrowsAny<XunitException>(() =>
            SnowflakeDbExceptionAssert.HasMessageInExceptionChain(exception, "not in the message"));
    }

    [SFFact]
    public void TestHasMessageInExceptionChain_NullException_Throws()
    {
        // act & assert
        Assert.ThrowsAny<XunitException>(() =>
            SnowflakeDbExceptionAssert.HasMessageInExceptionChain(null, "anything"));
    }
}
