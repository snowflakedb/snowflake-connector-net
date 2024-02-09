/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

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

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// OktaAuthenticator would perform serveral steps of authentication with Snowflake and Okta idp
    /// </summary>
    class OktaAuthenticator : BaseAuthenticator, IAuthenticator
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<OktaAuthenticator>();

        internal const string RetryCountHeader = "RetryCount";
        internal const string TimeoutElapsedHeader = "TimeoutElapsed";

        /// <summary>
        /// url of the okta idp
        /// </summary>
        private Uri oktaUrl;

        // The raw Saml token.
        private string samlRawHtmlString;

        /// <summary>
        /// Constructor of the Okta authenticator
        /// </summary>
        /// <param name="session"></param>
        /// <param name="oktaUriString"></param>
        internal OktaAuthenticator(SFSession session, string oktaUriString) : 
            base(session, oktaUriString)
        {
            oktaUrl = new Uri(oktaUriString);
        }

        /// <see cref="IAuthenticator"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            logger.Info("Okta Authentication");

            logger.Debug("step 1: get sso and token url");
            var authenticatorRestRequest = BuildAuthenticatorRestRequest();
            var authenticatorResponse = await session.restRequester.PostAsync<AuthenticatorResponse>(authenticatorRestRequest, cancellationToken).ConfigureAwait(false);
            authenticatorResponse.FilterFailedResponse();
            Uri ssoUrl = new Uri(authenticatorResponse.data.ssoUrl);
            Uri tokenUrl = new Uri(authenticatorResponse.data.tokenUrl);

            logger.Debug("step 2: verify urls fetched from step 1");
            logger.Debug("Checking sso url");
            VerifyUrls(ssoUrl, oktaUrl);
            logger.Debug("Checking token url");
            VerifyUrls(tokenUrl, oktaUrl);

            int retryCount = 0;
            int timeoutElapsed = 0;
            Exception lastRetryException = null;
            HttpResponseMessage samlRawResponse = null;

            // If VerifyPostbackUrl() fails, retry with new onetimetoken
            while (RetryLimitIsNotReached(retryCount, timeoutElapsed))
            {
                try
                {
                    logger.Debug("step 3: get idp onetime token");
                    IdpTokenRestRequest idpTokenRestRequest = BuildIdpTokenRestRequest(tokenUrl);
                    var idpResponse = await session.restRequester.PostAsync<IdpTokenResponse>(idpTokenRestRequest, cancellationToken).ConfigureAwait(false);
                    string onetimeToken = idpResponse.SessionToken != null ? idpResponse.SessionToken : idpResponse.CookieToken;

                    logger.Debug("step 4: get SAML reponse from sso");
                    var samlRestRequest = BuildSAMLRestRequest(ssoUrl, onetimeToken);
                    samlRawResponse = await session.restRequester.GetAsync(samlRestRequest, cancellationToken).ConfigureAwait(false);
                    samlRawHtmlString = await samlRawResponse.Content.ReadAsStringAsync().ConfigureAwait(false);

                    logger.Debug("step 5: verify postback url in SAML reponse");
                    VerifyPostbackUrl();

                    logger.Debug("step 6: send SAML reponse to snowflake to login");
                    await base.LoginAsync(cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex)
                {
                    lastRetryException = ex;
                    if (IsPostbackUrlNotFound(lastRetryException))
                    {
                        logger.Info("Refreshing token for Okta re-authentication and starting from step 3 again");

                        // Get the current retry count and timeout elapsed from the response headers
                        retryCount += int.Parse(samlRawResponse.Content.Headers.GetValues(RetryCountHeader).First());
                        timeoutElapsed += int.Parse(samlRawResponse.Content.Headers.GetValues(TimeoutElapsedHeader).First());
                    }
                    else
                    {
                        logger.Error("Failed to get the correct SAML response from Okta", ex);
                        throw;
                    }
                }
            }

            // Throw exception if max retry count or max timeout has been reached
            ThrowRetryLimitException(retryCount, timeoutElapsed, lastRetryException);
        }

        void IAuthenticator.Authenticate()
        {
            logger.Info("Okta Authentication");

            logger.Debug("step 1: get sso and token url");
            var authenticatorRestRequest = BuildAuthenticatorRestRequest();
            var authenticatorResponse = session.restRequester.Post<AuthenticatorResponse>(authenticatorRestRequest);
            authenticatorResponse.FilterFailedResponse();
            Uri ssoUrl = new Uri(authenticatorResponse.data.ssoUrl);
            Uri tokenUrl = new Uri(authenticatorResponse.data.tokenUrl);

            logger.Debug("step 2: verify urls fetched from step 1");
            logger.Debug("Checking sso url");
            VerifyUrls(ssoUrl, oktaUrl);
            logger.Debug("Checking token url");
            VerifyUrls(tokenUrl, oktaUrl);

            int retryCount = 0;
            int timeoutElapsed = 0;
            Exception lastRetryException = null;
            HttpResponseMessage samlRawResponse = null;

            // If VerifyPostbackUrl() fails, retry with new onetimetoken
            while (RetryLimitIsNotReached(retryCount, timeoutElapsed))
            {
                try
                {
                    logger.Debug("step 3: get idp onetime token");
                    IdpTokenRestRequest idpTokenRestRequest = BuildIdpTokenRestRequest(tokenUrl);
                    var idpResponse =  session.restRequester.Post<IdpTokenResponse>(idpTokenRestRequest);
                    string onetimeToken = idpResponse.SessionToken != null ? idpResponse.SessionToken : idpResponse.CookieToken;

                    logger.Debug("step 4: get SAML reponse from sso");
                    var samlRestRequest = BuildSAMLRestRequest(ssoUrl, onetimeToken);
                    samlRawResponse = session.restRequester.Get(samlRestRequest);
                    samlRawHtmlString = Task.Run(async () => await samlRawResponse.Content.ReadAsStringAsync().ConfigureAwait(false)).Result;

                    logger.Debug("step 5: verify postback url in SAML reponse");
                    VerifyPostbackUrl();


                    logger.Debug("step 6: send SAML reponse to snowflake to login");
                    base.Login();
                    break;
                }
                catch(Exception ex)
                {
                    lastRetryException = ex;
                    if (IsPostbackUrlNotFound(lastRetryException))
                    {
                        logger.Debug("Refreshing token for Okta re-authentication and starting from step 3 again");

                        // Get the current retry count and timeout elapsed from the response headers
                        retryCount += int.Parse(samlRawResponse.Content.Headers.GetValues(RetryCountHeader).First());
                        timeoutElapsed += int.Parse(samlRawResponse.Content.Headers.GetValues(TimeoutElapsedHeader).First());
                    }
                    else
                    {
                        logger.Error("Failed to get the correct SAML response from Okta", ex);
                        throw;
                    }
                }
            }

            // Throw exception if max retry count or max timeout has been reached
            ThrowRetryLimitException(retryCount, timeoutElapsed, lastRetryException);
        }

        private SFRestRequest BuildAuthenticatorRestRequest()
        {
            var fedUrl = session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);
            var data = new AuthenticatorRequestData()
            {
                AccountName = session.properties[SFSessionProperty.ACCOUNT],
                Authenticator = oktaUrl.ToString(),
                DriverVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString(),
                DriverName = ".NET"
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(fedUrl, new AuthenticatorRequest() { Data = data });
        }

        private IdpTokenRestRequest BuildIdpTokenRestRequest(Uri tokenUrl)
        {
            return new IdpTokenRestRequest()
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

        private SAMLRestRequest BuildSAMLRestRequest(Uri ssoUrl, string onetimeToken)
        {
            return new SAMLRestRequest()
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
            data.RawSamlResponse = samlRawHtmlString;
        }

        private void VerifyUrls(Uri tokenOrSsoUrl, Uri sessionUrl)
        {
            if (tokenOrSsoUrl.Scheme != sessionUrl.Scheme || tokenOrSsoUrl.Host != sessionUrl.Host)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_SSO_TOKEN_URL_MISMATCH, tokenOrSsoUrl.ToString(), oktaUrl.ToString());
                logger.Error("Different urls", e);
                throw e;
            }
        }

        private void VerifyPostbackUrl()
        {
            int formIndex = samlRawHtmlString.IndexOf("<form");
            bool extractSuccess = formIndex == -1;

            // skip 'action="' (length = 8)
            int startIndex = samlRawHtmlString.IndexOf("action=", formIndex) + 8;
            int length = samlRawHtmlString.IndexOf('"', startIndex) - startIndex;

            Uri postBackUrl;
            try
            {
                postBackUrl = new Uri(HttpUtility.HtmlDecode(samlRawHtmlString.Substring(startIndex, length)));
            } catch (Exception e)
            {
                logger.Error("Fail to extract SAML from html", e);
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
                logger.Error("Different urls", e);
                throw e;
            }
        }

        private void FilterFailedResponse(BaseRestResponse response)
        {
            if (!response.success)
            {
                SnowflakeDbException e = new SnowflakeDbException("", response.code, response.message, "");
                logger.Error("Authentication failed", e);
                throw e;
            }
        }

        private bool RetryLimitIsNotReached(int retryCount, int timeoutElapsed)
        {
            return retryCount < session._maxRetryCount && timeoutElapsed < session._maxRetryTimeout;
        }

        private bool IsPostbackUrlNotFound(Exception ex)
        {
            if (ex is SnowflakeDbException)
            {
                SnowflakeDbException error = ex as SnowflakeDbException;
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
            if (timeoutElapsed >= session._maxRetryTimeout)
            {
                errorMessage += string.IsNullOrEmpty(errorMessage) ? "The" : " and the";
                errorMessage += $" timeout elapsed has reached its limit of {session._maxRetryTimeout}";

            }
            errorMessage += " while trying to authenticate through Okta";

            logger.Error(errorMessage);
            throw new SnowflakeDbException(lastRetryException, SFError.INTERNAL_ERROR, errorMessage);
        }
    }

    internal class IdpTokenRestRequest : BaseRestRequest, IRestRequest
    {   
        private static MediaTypeWithQualityHeaderValue jsonHeader = new MediaTypeWithQualityHeaderValue("application/json");

        internal IdpTokenRequest JsonBody { get; set; }
            
        HttpRequestMessage IRestRequest.ToRequestMessage(HttpMethod method)
        {
            HttpRequestMessage message = newMessage(method, Url);
            message.Headers.Accept.Add(jsonHeader);

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

    class SAMLRestRequest : BaseRestRequest, IRestRequest
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
