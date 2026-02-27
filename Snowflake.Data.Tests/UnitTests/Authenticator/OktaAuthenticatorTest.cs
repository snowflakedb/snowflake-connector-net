using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.Okta;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class OktaAuthenticatorTest
    {
        private const string OktaUrl = "https://snowflakecomputing.okta.com";
        private const string TokenUrl = "https://snowflakecomputing.okta.com/api/v1/sessions?additionalFields=cookieToken";
        private const string SsoUrl = "https://snowflakecomputing.okta.com/app/snowflake/sso/saml";
        private const string ValidSamlHtml = "<form action=\"https://test.snowflakecomputing.com/fed/login\">";
        private const int MaxRetryCount = 15;
        private const int MaxRetryTimeout = 400;

        [Test]
        [TestCase("https://xxxxxx.okta.com", true)]
        [TestCase("https://xxxxxx.oktapreview.com", true)]
        [TestCase("https://vanity.url/snowflake/okta", true)]
        [TestCase("http://xxxxxx.okta.com", false)]
        [TestCase("https://xxxxxx.com", false)]
        [TestCase("username_password_mfa", false)]
        public void TestRecognizeOktaAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = OktaAuthenticator.IsOktaAuthenticator(authenticator);

            // assert
            Assert.AreEqual(expectedResult, result);
        }

        [Test]
        public async Task TestSuccessfulAuthenticationAsync()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            var postbackUrl = new Uri("https://test.snowflakecomputing.com/fed/login");
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>())).Returns(postbackUrl);

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act
            await ((IAuthenticator)authenticator).AuthenticateAsync(CancellationToken.None);

            // assert
            mockUrlValidator.Verify(v => v.ValidateTokenOrSsoUrl(It.IsAny<Uri>(), It.IsAny<Uri>()), Times.Exactly(2));
            mockSamlParser.Verify(p => p.ExtractPostbackUrl(ValidSamlHtml), Times.Once);
            mockUrlValidator.Verify(v => v.ValidatePostbackUrl(postbackUrl, "test.snowflakecomputing.com", "https"), Times.Once);
        }

        [Test]
        public void TestSuccessfulAuthenticationSync()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            var postbackUrl = new Uri("https://test.snowflakecomputing.com/fed/login");
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>())).Returns(postbackUrl);

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act
            ((IAuthenticator)authenticator).Authenticate();

            // assert
            mockUrlValidator.Verify(v => v.ValidateTokenOrSsoUrl(It.IsAny<Uri>(), It.IsAny<Uri>()), Times.Exactly(2));
            mockSamlParser.Verify(p => p.ExtractPostbackUrl(ValidSamlHtml), Times.Once);
            mockUrlValidator.Verify(v => v.ValidatePostbackUrl(postbackUrl, "test.snowflakecomputing.com", "https"), Times.Once);
        }

        [Test]
        public void TestUrlValidatorCalledForSsoAndTokenUrls()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>()))
                .Returns(new Uri("https://test.snowflakecomputing.com/fed/login"));

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act
            ((IAuthenticator)authenticator).Authenticate();

            // assert - verify SSO and token URL validation
            mockUrlValidator.Verify(
                v => v.ValidateTokenOrSsoUrl(new Uri(SsoUrl), new Uri(OktaUrl)),
                Times.Once);
            mockUrlValidator.Verify(
                v => v.ValidateTokenOrSsoUrl(new Uri(TokenUrl), new Uri(OktaUrl)),
                Times.Once);
        }

        [Test]
        public void TestUrlMismatchThrowsException()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = "https://different.okta.com/api/v1/sessions",
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            mockUrlValidator.Setup(v => v.ValidateTokenOrSsoUrl(It.IsAny<Uri>(), It.IsAny<Uri>()))
                .Callback<Uri, Uri>((tokenOrSsoUrl, oktaUrl) =>
                {
                    if (tokenOrSsoUrl.Host != oktaUrl.Host)
                    {
                        throw new SnowflakeDbException(SFError.IDP_SSO_TOKEN_URL_MISMATCH, tokenOrSsoUrl.ToString(), oktaUrl.ToString());
                    }
                });

            var mockSamlParser = new Mock<ISamlResponseParser>();

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act & assert - using GetAwaiter().GetResult() preserves the original exception type
            var ex = Assert.Throws<SnowflakeDbException>(() => ((IAuthenticator)authenticator).Authenticate());
            Assert.AreEqual(SFError.IDP_SSO_TOKEN_URL_MISMATCH.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }

        [Test]
        public void TestRetryBehaviorWhenPostbackUrlNotFound()
        {
            // arrange
            var callCount = 0;
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            // First call throws, second call succeeds
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>()))
                .Returns(() =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        throw new SnowflakeDbException(new Exception("parsing failed"), SFError.IDP_SAML_POSTBACK_NOTFOUND);
                    }
                    return new Uri("https://test.snowflakecomputing.com/fed/login");
                });

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act
            ((IAuthenticator)authenticator).Authenticate();

            // assert - parser called twice due to retry
            Assert.AreEqual(2, callCount);
            mockSamlParser.Verify(p => p.ExtractPostbackUrl(It.IsAny<string>()), Times.Exactly(2));
        }

        [Test]
        public void TestMaxRetryLimitExceeded()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = MaxRetryCount,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            // Always throw to trigger retries
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>()))
                .Throws(new SnowflakeDbException(new Exception("parsing failed"), SFError.IDP_SAML_POSTBACK_NOTFOUND));

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act & assert - using GetAwaiter().GetResult() preserves the original exception type
            var ex = Assert.Throws<SnowflakeDbException>(() => ((IAuthenticator)authenticator).Authenticate());
            Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
            Assert.That(ex.Message, Does.Contain("retry count has reached its limit"));
        }

        [Test]
        public void TestTimeoutExceeded()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 500 // Set a high timeout that will exceed the session limit
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            // Always throw to trigger retries
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>()))
                .Throws(new SnowflakeDbException(new Exception("parsing failed"), SFError.IDP_SAML_POSTBACK_NOTFOUND));

            // Use a very short retry timeout
            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES=100;RETRY_TIMEOUT=1;",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act & assert - using GetAwaiter().GetResult() preserves the original exception type
            var ex = Assert.Throws<SnowflakeDbException>(() => ((IAuthenticator)authenticator).Authenticate());
            Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
            Assert.That(ex.Message, Does.Contain("timeout elapsed has reached its limit"));
        }

        [Test]
        public void TestPostbackUrlMismatchThrowsException()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            var wrongPostbackUrl = new Uri("https://wrong.host.com/fed/login");
            mockSamlParser.Setup(p => p.ExtractPostbackUrl(It.IsAny<string>())).Returns(wrongPostbackUrl);

            mockUrlValidator.Setup(v => v.ValidatePostbackUrl(wrongPostbackUrl, It.IsAny<string>(), It.IsAny<string>()))
                .Throws(new SnowflakeDbException(SFError.IDP_SAML_POSTBACK_INVALID, wrongPostbackUrl.ToString(), "https:\\\\test.snowflakecomputing.com"));

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act & assert - using GetAwaiter().GetResult() preserves the original exception type
            var ex = Assert.Throws<SnowflakeDbException>(() => ((IAuthenticator)authenticator).Authenticate());
            Assert.AreEqual(SFError.IDP_SAML_POSTBACK_INVALID.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }

        [Test]
        public void TestSamlUrlCheckDisabledSkipsValidation()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = ValidSamlHtml,
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var mockUrlValidator = new Mock<IOktaUrlValidator>();
            var mockSamlParser = new Mock<ISamlResponseParser>();

            // Session with SAML URL check disabled (property name is DISABLE_SAML_URL_CHECK)
            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};DISABLE_SAML_URL_CHECK=true;",
                new SessionPropertiesContext(),
                mockRestRequester);

            var authenticator = new OktaAuthenticator(session, OktaUrl, mockUrlValidator.Object, mockSamlParser.Object);

            // act
            ((IAuthenticator)authenticator).Authenticate();

            // assert - SAML parser should NOT be called when check is disabled
            mockSamlParser.Verify(p => p.ExtractPostbackUrl(It.IsAny<string>()), Times.Never);
            mockUrlValidator.Verify(v => v.ValidatePostbackUrl(It.IsAny<Uri>(), It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        }

        [Test]
        public void TestDefaultConstructorUsesConcreteImplementations()
        {
            // arrange
            var mockRestRequester = new MockOktaRestRequester
            {
                TokenUrl = TokenUrl,
                SSOUrl = SsoUrl,
                ResponseContent = "<form action=\"https://test.snowflakecomputing.com/fed/login\">",
                MaxRetryCount = 1,
                MaxRetryTimeout = 0
            };

            var session = new SFSession(
                $"account=test;user=test;password=test;authenticator={OktaUrl};host=test.snowflakecomputing.com;MAXHTTPRETRIES={MaxRetryCount};RETRY_TIMEOUT={MaxRetryTimeout};",
                new SessionPropertiesContext(),
                mockRestRequester);

            // act - use default constructor (no injected dependencies)
            var authenticator = new OktaAuthenticator(session, OktaUrl);

            // assert - should succeed with concrete implementations
            Assert.DoesNotThrow(() => ((IAuthenticator)authenticator).Authenticate());
        }
    }
}
