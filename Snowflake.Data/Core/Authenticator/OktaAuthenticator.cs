using System;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Text;
using System.Web;
using System.Linq;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// OktaAuthenticator would perform several steps of authentication with Snowflake and Okta IdP
    /// </summary>
    class OktaAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AUTH_NAME = "okta";
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OktaAuthenticator>();

        internal const string RetryCountHeader = "RetryCount";
        internal const string TimeoutElapsedHeader = "TimeoutElapsed";

        /// <summary>
        /// url of the okta idp
        /// </summary>
        private readonly Uri _oktaUrl;

        private string _rawSamlTokenHtmlString;

        /// <summary>
        /// Constructor of the Okta authenticator
        /// </summary>
        /// <param name="session"></param>
        /// <param name="oktaUriString"></param>
        internal OktaAuthenticator(SFSession session, string oktaUriString) :
            base(session, oktaUriString)
        {
            _oktaUrl = new Uri(oktaUriString);
        }

        public static bool IsOktaAuthenticator(string authenticator) =>
            authenticator.Contains("okta") && authenticator.StartsWith("https://");

        /// <see cref="IAuthenticator"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            s_logger.Info("Okta Authentication");

            s_logger.Debug("step 1: Get SSO and token URL");
            var authenticatorRestRequest = BuildAuthenticatorRestRequest();
            var authenticatorResponse = await session.restRequester.PostAsync<AuthenticatorResponse>(authenticatorRestRequest, cancellationToken).ConfigureAwait(false);
            authenticatorResponse.FilterFailedResponse();
            Uri ssoUrl = new Uri(authenticatorResponse.data.ssoUrl);
            Uri tokenUrl = new Uri(authenticatorResponse.data.tokenUrl);

            s_logger.Debug("step 2: Verify URLs fetched from step 1");
            s_logger.Debug("Checking SSO Okta URL");
            VerifyUrls(ssoUrl, _oktaUrl);
            s_logger.Debug("Checking token URL");
            VerifyUrls(tokenUrl, _oktaUrl);

            int retryCount = 0;
            int timeoutElapsed = 0;
            Exception lastRetryException = null;
            HttpResponseMessage samlRawResponse = null;

            // If VerifyPostbackUrl() fails, retry with new one-time token
            while (RetryLimitIsNotReached(retryCount, timeoutElapsed))
            {
                try
                {
                    s_logger.Debug("step 3: Get IdP one-time token");
                    IdpTokenRestRequest idpTokenRestRequest = BuildIdpTokenRestRequest(tokenUrl);
                    var idpResponse = await session.restRequester.PostAsync<IdpTokenResponse>(idpTokenRestRequest, cancellationToken).ConfigureAwait(false);
                    string onetimeToken = idpResponse.SessionToken ?? idpResponse.CookieToken;

                    s_logger.Debug("step 4: Get SAML response from SSO");
                    var samlRestRequest = BuildSamlRestRequest(ssoUrl, onetimeToken);
                    samlRawResponse = await session.restRequester.GetAsync(samlRestRequest, cancellationToken).ConfigureAwait(false);
                    _rawSamlTokenHtmlString = await samlRawResponse.Content.ReadAsStringAsync().ConfigureAwait(false);
                    s_logger.Debug("step 5: Verify postback URL in SAML response");
                    if (!session._disableSamlUrlCheck)
                    {
                        VerifyPostbackUrl();
                    }
                    else
                    {
                        s_logger.Debug("The saml url check is disabled. Skipping step 5");
                    }

                    s_logger.Debug("step 6: Send SAML response to Snowflake to login");
                    await LoginAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch (Exception ex)
                {
                    lastRetryException = ex;
                    HandleAuthenticatorException(ex, samlRawResponse, ref retryCount, ref timeoutElapsed);
                }
                finally
                {
                    samlRawResponse?.Dispose();
                    samlRawResponse = null;
                }
            } // while retry

            // Throw exception if max retry count or max timeout has been reached
            ThrowRetryLimitException(retryCount, timeoutElapsed, lastRetryException);
        }

        void IAuthenticator.Authenticate()
        {
            s_logger.Info("Okta Authentication");

            s_logger.Debug("step 1: Get SSO and token URL");
            var authenticatorRestRequest = BuildAuthenticatorRestRequest();
            var authenticatorResponse = session.restRequester.Post<AuthenticatorResponse>(authenticatorRestRequest);
            authenticatorResponse.FilterFailedResponse();
            Uri ssoUrl = new Uri(authenticatorResponse.data.ssoUrl);
            Uri tokenUrl = new Uri(authenticatorResponse.data.tokenUrl);

            s_logger.Debug("step 2: Verify URLs fetched from step 1");
            s_logger.Debug("Checking SSO Okta URL");
            VerifyUrls(ssoUrl, _oktaUrl);
            s_logger.Debug("Checking token URL");
            VerifyUrls(tokenUrl, _oktaUrl);

            int retryCount = 0;
            int timeoutElapsed = 0;
            Exception lastRetryException = null;
            HttpResponseMessage samlRawResponse = null;

            // If VerifyPostbackUrl() fails, retry with new one-time token
            while (RetryLimitIsNotReached(retryCount, timeoutElapsed))
            {
                try
                {
                    s_logger.Debug("step 3: Get IdP one-time token");
                    IdpTokenRestRequest idpTokenRestRequest = BuildIdpTokenRestRequest(tokenUrl);
                    var idpResponse = session.restRequester.Post<IdpTokenResponse>(idpTokenRestRequest);
                    string onetimeToken = idpResponse.SessionToken ?? idpResponse.CookieToken;

                    s_logger.Debug("step 4: Get SAML response from SSO");
                    var samlRestRequest = BuildSamlRestRequest(ssoUrl, onetimeToken);
                    samlRawResponse = session.restRequester.Get(samlRestRequest);
                    _rawSamlTokenHtmlString = Task.Run(async () => await samlRawResponse.Content.ReadAsStringAsync().ConfigureAwait(false)).Result;

                    s_logger.Debug("step 5: Verify postback URL in SAML response");
                    if (!session._disableSamlUrlCheck)
                    {
                        VerifyPostbackUrl();
                    }
                    else
                    {
                        s_logger.Debug("The saml url check is disabled. Skipping step 5");
                    }

                    s_logger.Debug("step 6: Send SAML response to Snowflake to login");
                    Login();
                    return;
                }
                catch (Exception ex)
                {
                    lastRetryException = ex;
                    HandleAuthenticatorException(ex, samlRawResponse, ref retryCount, ref timeoutElapsed);
                }
                finally
                {
                    samlRawResponse?.Dispose();
                    samlRawResponse = null;
                }
            } // while retry

            // Throw exception if max retry count or max timeout has been reached
            ThrowRetryLimitException(retryCount, timeoutElapsed, lastRetryException);
        }

        private void HandleAuthenticatorException(Exception ex, HttpResponseMessage samlRawResponse, ref int retryCount, ref int timeoutElapsed)
        {
            if (IsPostbackUrlNotFound(ex))
            {
                s_logger.Debug("Refreshing token for Okta re-authentication and starting from step 3 again");

                if (samlRawResponse is null)
                {
                    var errorNullSamlResponse = "Failure getting SAML response from Okta SSO";
                    s_logger.Error(errorNullSamlResponse);
                    throw new SnowflakeDbException(ex, SFError.IDP_SAML_POSTBACK_INVALID);
                }

                // Get the current retry count and timeout elapsed from the response headers
                retryCount += int.Parse(samlRawResponse.Content.Headers.GetValues(RetryCountHeader).First());
                timeoutElapsed += int.Parse(samlRawResponse.Content.Headers.GetValues(TimeoutElapsedHeader).First());
            }
            else
            {
                s_logger.Error("Failed to get the correct SAML response from Okta SSO", ex);
                throw ex;
            }
        }

        private SFRestRequest BuildAuthenticatorRestRequest()
        {
            var fedUrl = session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);
            var data = new AuthenticatorRequestData
            {
                AccountName = session.properties[SFSessionProperty.ACCOUNT],
                Authenticator = _oktaUrl.ToString(),
                DriverVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString(),
                DriverName = ".NET"
            };

            return session.BuildTimeoutRestRequest(fedUrl, new AuthenticatorRequest { Data = data });
        }

        private IdpTokenRestRequest BuildIdpTokenRestRequest(Uri tokenUrl)
        {
            return new IdpTokenRestRequest
            {
                Url = tokenUrl,
                RestTimeout = session.connectionTimeout,
                HttpTimeout = TimeSpan.FromSeconds(16),
                JsonBody = new IdpTokenRequest()
                {
                    Username = session.properties[SFSessionProperty.USER],
                    Password = session.properties[SFSessionProperty.PASSWORD],
                },
            };
        }

        private SamlRestRequest BuildSamlRestRequest(Uri ssoUrl, string onetimeToken)
        {
            return new SamlRestRequest()
            {
                Url = ssoUrl,
                RestTimeout = session.connectionTimeout,
                HttpTimeout = Timeout.InfiniteTimeSpan,
                OnetimeToken = onetimeToken,
            };
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.RawSamlResponse = _rawSamlTokenHtmlString;
            SetSecondaryAuthenticationData(ref data);
        }

        private void VerifyUrls(Uri tokenOrSsoUrl, Uri sessionUrl)
        {
            if (tokenOrSsoUrl.Scheme != sessionUrl.Scheme || tokenOrSsoUrl.Host != sessionUrl.Host)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_SSO_TOKEN_URL_MISMATCH, tokenOrSsoUrl.ToString(), _oktaUrl.ToString());
                s_logger.Error("Different urls", e);
                throw e;
            }
        }

        private void VerifyPostbackUrl()
        {
            int formIndex = _rawSamlTokenHtmlString.IndexOf("<form");

            // skip 'action="' (length = 8)
            int startIndex = _rawSamlTokenHtmlString.IndexOf("action=", formIndex) + 8;
            int length = _rawSamlTokenHtmlString.IndexOf('"', startIndex) - startIndex;

            Uri postBackUrl;
            try
            {
                postBackUrl = new Uri(HttpUtility.HtmlDecode(_rawSamlTokenHtmlString.Substring(startIndex, length)));
            }
            catch (Exception e)
            {
                s_logger.Error("Fail to extract SAML from html", e);
                throw new SnowflakeDbException(e, SFError.IDP_SAML_POSTBACK_NOTFOUND);
            }

            string sessionHost = session.properties[SFSessionProperty.HOST];
            string sessionScheme = session.properties[SFSessionProperty.SCHEME];
            if (postBackUrl.Host != sessionHost ||
                postBackUrl.Scheme != sessionScheme)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_SAML_POSTBACK_INVALID,
                    postBackUrl.ToString(),
                    sessionScheme + ":\\\\" + sessionHost);
                s_logger.Error("Different urls", e);
                throw e;
            }
        }

        private bool RetryLimitIsNotReached(int retryCount, int timeoutElapsed)
        {
            var elapsedMillis = timeoutElapsed * 1000;
            return retryCount < session._maxRetryCount && !TimeoutHelper.IsExpired(elapsedMillis, session._maxRetryTimeout);
        }

        private bool IsPostbackUrlNotFound(Exception ex)
        {
            if (ex is SnowflakeDbException error)
            {
                return error.ErrorCode == SFError.IDP_SAML_POSTBACK_NOTFOUND.GetAttribute<SFErrorAttr>().errorCode;
            }

            return false;
        }

        private void ThrowRetryLimitException(int retryCount, int timeoutElapsed, Exception lastRetryException)
        {
            string errorMessage = "";
            if (retryCount >= session._maxRetryCount)
            {
                errorMessage = $"The retry count has reached its limit of {session._maxRetryCount}";
            }
            if (TimeoutHelper.IsExpired(timeoutElapsed * 1000, session._maxRetryTimeout))
            {
                errorMessage += string.IsNullOrEmpty(errorMessage) ? "The" : " and the";
                errorMessage += $" timeout elapsed has reached its limit of {session._maxRetryTimeout.TotalSeconds}";

            }
            errorMessage += " while trying to authenticate through Okta";

            s_logger.Error(errorMessage);
            throw new SnowflakeDbException(lastRetryException, SFError.INTERNAL_ERROR, errorMessage);
        }
    }

    internal class IdpTokenRestRequest : BaseRestRequest, IRestRequest
    {
        private static readonly MediaTypeWithQualityHeaderValue s_jsonHeader = new MediaTypeWithQualityHeaderValue("application/json");

        internal IdpTokenRequest JsonBody { get; set; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            HttpRequestMessage message = newMessage(method, Url);
            message.Headers.Accept.Add(s_jsonHeader);

            var json = JsonConvert.SerializeObject(JsonBody, JsonUtils.JsonSettings);
            message.Content = new StringContent(json, Encoding.UTF8, "application/json");

            return message;
        }
    }

    class IdpTokenRequest
    {
        [JsonProperty(PropertyName = "username")]
        internal String Username { get; set; }

        [JsonProperty(PropertyName = "password")]
        internal String Password { get; set; }
    }

    class IdpTokenResponse
    {
        [JsonProperty(PropertyName = "cookieToken")]
        internal String CookieToken { get; set; }
        [JsonProperty(PropertyName = "sessionToken")]
        internal String SessionToken { get; set; }
    }

    class SamlRestRequest : BaseRestRequest, IRestRequest
    {
        internal string OnetimeToken { set; get; }

        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            UriBuilder builder = new UriBuilder(Url);
            builder.Query = "RelayState=%2Fsome%2Fdeep%2Flink&onetimetoken=" + OnetimeToken;
            HttpRequestMessage message = newMessage(method, builder.Uri);

            message.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));

            return message;
        }
    }
}
