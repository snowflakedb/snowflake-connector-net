using System;
using Xunit;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class ExternalBrowserAuthenticatorTest
    {
        [SFTheory]
        [InlineData("externalbrowser", true)]
        [InlineData("EXTERNALBROWSER", true)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeExternalBrowserAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = ExternalBrowserAuthenticator.IsExternalBrowserAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }

        [SFTheory]
        [InlineData("https://account.snowflakecomputing.com", true)]
        [InlineData("https://account.snowflakecomputing.com:443", true)]
        [InlineData("HTTPS://ACCOUNT.SNOWFLAKECOMPUTING.COM", true)]
        [InlineData("http://account.snowflakecomputing.com", false)]
        [InlineData("https://other.snowflakecomputing.com", false)]
        [InlineData("https://account.snowflakecomputing.com:444", false)]
        [InlineData("https://account.snowflakecomputing.com:8080", false)]
        [InlineData("https://account.snowflakecomputing.com.example.com", false)]
        [InlineData("https://account.snowflakecomputing.com/path", false)]
        [InlineData("https://account.snowflakecomputing.com?param=1", false)]
        [InlineData("https://user@account.snowflakecomputing.com", false)]
        [InlineData("null", false)]
        [InlineData(null, false)]
        [InlineData("", false)]
        public void TestMatchingCallbackOrigin(string origin, bool expectedResult)
        {
            // arrange
            var accountUrl = new Uri("https://account.snowflakecomputing.com:443");

            // act
            var result = ExternalBrowserAuthenticator.OriginMatchesAccount(origin, accountUrl);

            // assert
            Assert.Equal(expectedResult, result);
        }

        [SFTheory]
        [InlineData("http://account.snowflakecomputing.com", true)]
        [InlineData("http://account.snowflakecomputing.com:80", true)]
        [InlineData("http://account.snowflakecomputing.com:81", false)]
        public void TestMatchingCallbackOriginWithDefaultHttpPort(string origin, bool expectedResult)
        {
            // arrange
            var accountUrl = new Uri("http://account.snowflakecomputing.com:80");

            // act
            var result = ExternalBrowserAuthenticator.OriginMatchesAccount(origin, accountUrl);

            // assert
            Assert.Equal(expectedResult, result);
        }

        [SFTheory]
        [InlineData("https://account.snowflakecomputing.com:8080", true)]
        [InlineData("https://account.snowflakecomputing.com", false)]
        [InlineData("https://account.snowflakecomputing.com:443", false)]
        public void TestMatchingCallbackOriginWithNonDefaultHttpsPort(string origin, bool expectedResult)
        {
            // arrange
            var accountUrl = new Uri("https://account.snowflakecomputing.com:8080");

            // act
            var result = ExternalBrowserAuthenticator.OriginMatchesAccount(origin, accountUrl);

            // assert
            Assert.Equal(expectedResult, result);
        }

        [SFTheory]
        [InlineData("{\"token\":\"json_token\",\"consent\":true}", "json_token")]
        [InlineData("{\"token\":\"tk2\"}", "tk2")]
        [InlineData("token=form_token&extra=val", "form_token")]
        [InlineData("{\"token\":\"\"}", null)]
        [InlineData("", null)]
        public void TestExtractTokenFromPostBody(string body, string expectedToken)
        {
            // act
            var result = ExternalBrowserAuthenticator.TryExtractTokenFromPost(body);

            // assert
            Assert.Equal(expectedToken, result);
        }
    }
}
