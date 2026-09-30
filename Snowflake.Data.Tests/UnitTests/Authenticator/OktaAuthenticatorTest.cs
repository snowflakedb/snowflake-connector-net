using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RichardSzalay.MockHttp;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class OktaAuthenticatorTest
    {
        [SFTheory]
        [InlineData("https://xxxxxx.okta.com", true)]
        [InlineData("https://xxxxxx.oktapreview.com", true)]
        [InlineData("https://vanity.url/snowflake/okta", true)]
        [InlineData("http://xxxxxx.okta.com", false)]
        [InlineData("https://xxxxxx.com", false)]
        [InlineData("username_password_mfa", false)]
        public void TestRecognizeOktaAuthenticator(string authenticator, bool expectedResult)
        {
            // act
            var result = OktaAuthenticator.IsOktaAuthenticator(authenticator);

            // assert
            Assert.Equal(expectedResult, result);
        }

        [SFFact]
        public async Task TestOktaAuthenticatorDoesNotFollowRedirectToDifferentOrigin()
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            // Step 1: authenticator request to Snowflake
            mockHttp.When(HttpMethod.Post, "https://testaccount.snowflakecomputing.com/session/authenticator-request")
                .Respond("application/json", JsonConvert.SerializeObject(new AuthenticatorResponse
                {
                    success = true,
                    data = new AuthenticatorResponseData
                    {
                        tokenUrl = "https://company.okta.com/api/v1/authn",
                        ssoUrl = "https://company.okta.com/sso/saml"
                    }
                }));

            // Step 3: Okta authn POST returns 307 redirect to attacker origin
            var oktaAuthnRequest = mockHttp.When(HttpMethod.Post, "https://company.okta.com/api/v1/authn")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri("https://attacker.com/steal");
                    return response;
                });

            // Target attacker endpoint
            var attackerRequest = mockHttp.When("https://attacker.com/*")
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var httpClient = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var restRequester = new DelegatingMockRestRequester(httpClient);
            var sfSession = new SFSession(
                "account=testaccount;user=testUser;password=testPassword;authenticator=https://company.okta.com;host=testaccount.snowflakecomputing.com",
                new SessionPropertiesContext(),
                restRequester);
            IAuthenticator authenticator = new OktaAuthenticator(sfSession, "https://company.okta.com");

            // act & assert: authentication fails with SecurityException, credentials never sent to attacker
            await Assert.ThrowsAsync<SecurityException>(() => authenticator.AuthenticateAsync(CancellationToken.None)).ConfigureAwait(false);
            Assert.Equal(1, mockHttp.GetMatchCount(oktaAuthnRequest));
            Assert.Equal(0, mockHttp.GetMatchCount(attackerRequest));
        }

        [SFFact]
        public async Task TestOktaAuthenticatorDoesNotFollowHttpDowngradeRedirect()
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://testaccount.snowflakecomputing.com/session/authenticator-request")
                .Respond("application/json", JsonConvert.SerializeObject(new AuthenticatorResponse
                {
                    success = true,
                    data = new AuthenticatorResponseData
                    {
                        tokenUrl = "https://company.okta.com/api/v1/authn",
                        ssoUrl = "https://company.okta.com/sso/saml"
                    }
                }));

            // Step 3: Okta authn POST returns 307 redirect to plain HTTP
            var oktaAuthnRequest = mockHttp.When(HttpMethod.Post, "https://company.okta.com/api/v1/authn")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri("http://company.okta.com/api/v1/authn");
                    return response;
                });

            var insecureRequest = mockHttp.When("http://company.okta.com/*")
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var httpClient = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var restRequester = new DelegatingMockRestRequester(httpClient);
            var sfSession = new SFSession(
                "account=testaccount;user=testUser;password=testPassword;authenticator=https://company.okta.com;host=testaccount.snowflakecomputing.com",
                new SessionPropertiesContext(),
                restRequester);
            IAuthenticator authenticator = new OktaAuthenticator(sfSession, "https://company.okta.com");

            // act & assert: authentication fails with SecurityException, credentials never sent over plain HTTP
            await Assert.ThrowsAsync<SecurityException>(() => authenticator.AuthenticateAsync(CancellationToken.None)).ConfigureAwait(false);
            Assert.Equal(1, mockHttp.GetMatchCount(oktaAuthnRequest));
            Assert.Equal(0, mockHttp.GetMatchCount(insecureRequest));
        }

        [SFFact]
        public void TestOktaAuthenticatorSynchronousDoesNotFollowRedirectToDifferentOrigin()
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://testaccount.snowflakecomputing.com/session/authenticator-request")
                .Respond("application/json", JsonConvert.SerializeObject(new AuthenticatorResponse
                {
                    success = true,
                    data = new AuthenticatorResponseData
                    {
                        tokenUrl = "https://company.okta.com/api/v1/authn",
                        ssoUrl = "https://company.okta.com/sso/saml"
                    }
                }));

            var oktaAuthnRequest = mockHttp.When(HttpMethod.Post, "https://company.okta.com/api/v1/authn")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri("https://attacker.com/steal");
                    return response;
                });

            var attackerRequest = mockHttp.When("https://attacker.com/*")
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var httpClient = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var restRequester = new DelegatingMockRestRequester(httpClient);
            var sfSession = new SFSession(
                "account=testaccount;user=testUser;password=testPassword;authenticator=https://company.okta.com;host=testaccount.snowflakecomputing.com",
                new SessionPropertiesContext(),
                restRequester);
            IAuthenticator authenticator = new OktaAuthenticator(sfSession, "https://company.okta.com");

            // act & assert: authentication fails with SecurityException, credentials never sent to attacker
            // The sync path wraps via Task.Run().Result, so SecurityException may be inside AggregateException
            var ex = Assert.ThrowsAny<Exception>(() => authenticator.Authenticate());
            Assert.IsType<SecurityException>(ex.GetBaseException());
            Assert.Equal(1, mockHttp.GetMatchCount(oktaAuthnRequest));
            Assert.Equal(0, mockHttp.GetMatchCount(attackerRequest));
        }
        [SFFact]
        public async Task TestOktaAuthenticatorAcceptsPostbackWithDefaultPortWhenNoPortSpecified()
        {
            // postback URL uses default HTTPS port 443, connection string omits port — should match
            await RunOktaPostbackValidationAsync(
                postbackUrl: "https://testaccount.snowflakecomputing.com/fed/login").ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestOktaAuthenticatorAcceptsPostbackWithDifferentHostCasing()
        {
            // IdP may return the host in different casing — comparison must be case-insensitive
            await RunOktaPostbackValidationAsync(
                postbackUrl: "https://TestAccount.Snowflakecomputing.COM/fed/login").ConfigureAwait(false);
        }

        private async Task RunOktaPostbackValidationAsync(string postbackUrl)
        {
            var mockHttp = new MockHttpMessageHandler();

            // Step 1: authenticator-request → tokenUrl + ssoUrl
            mockHttp.When(HttpMethod.Post, "https://testaccount.snowflakecomputing.com/session/authenticator-request")
                .Respond("application/json", JsonConvert.SerializeObject(new AuthenticatorResponse
                {
                    success = true,
                    data = new AuthenticatorResponseData
                    {
                        tokenUrl = "https://company.okta.com/api/v1/authn",
                        ssoUrl = "https://company.okta.com/sso/saml"
                    }
                }));

            // Step 3: IdP authn → one-time token
            mockHttp.When(HttpMethod.Post, "https://company.okta.com/api/v1/authn")
                .Respond("application/json", JsonConvert.SerializeObject(new IdpTokenResponse { SessionToken = "fake-token" }));

            // Step 4: SSO SAML → HTML with postback action URL under test
            mockHttp.When(HttpMethod.Get, "https://company.okta.com/sso/saml*")
                .Respond("text/html",
                    $"<html><body><form action=\"{postbackUrl}\"></form></body></html>");

            // Step 6: login-request
            mockHttp.When(HttpMethod.Post, "https://testaccount.snowflakecomputing.com/session/v1/login-request")
                .Respond("application/json", JsonConvert.SerializeObject(new LoginResponse
                {
                    success = true,
                    data = new LoginResponseData
                    {
                        token = "session-token",
                        masterToken = "master-token",
                        sessionId = "1",
                        authResponseSessionInfo = new SessionInfo(),
                        nameValueParameter = new List<NameValueParameter>()
                    }
                }));

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var httpClient = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var restRequester = new DelegatingMockRestRequester(httpClient);
            var sfSession = new SFSession(
                "account=testaccount;user=testUser;password=testPassword;authenticator=https://company.okta.com;host=testaccount.snowflakecomputing.com",
                new SessionPropertiesContext(),
                restRequester);
            IAuthenticator authenticator = new OktaAuthenticator(sfSession, "https://company.okta.com");

            await authenticator.AuthenticateAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
