using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using System.Linq;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Core.Authenticator.Okta;

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

        private readonly IOktaUrlValidator _urlValidator;
        private readonly ISamlResponseParser _samlParser;

        /// <summary>
        /// Constructor of the Okta authenticator
        /// </summary>
        /// <param name="session"></param>
        /// <param name="oktaUriString"></param>
        internal OktaAuthenticator(SFSession session, string oktaUriString) :
            this(session, oktaUriString, new OktaUrlValidator(), new SamlResponseParser())
        {
        }

        /// <summary>
        /// Internal constructor for testing with injected dependencies
        /// </summary>
        /// <param name="session"></param>
        /// <param name="oktaUriString"></param>
        /// <param name="urlValidator">The URL validator to use</param>
        /// <param name="samlParser">The SAML response parser to use</param>
        internal OktaAuthenticator(SFSession session, string oktaUriString, IOktaUrlValidator urlValidator, ISamlResponseParser samlParser) :
            base(session, oktaUriString)
        {
            _oktaUrl = new Uri(oktaUriString);
            _urlValidator = urlValidator;
            _samlParser = samlParser;
        }

        public static bool IsOktaAuthenticator(string authenticator) =>
            authenticator.Contains("okta") && authenticator.StartsWith("https://");

        /// <see cref="IAuthenticator"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            await AuthenticateCoreAsync(cancellationToken).ConfigureAwait(false);
        }

        void IAuthenticator.Authenticate()
        {
            // Run synchronous in a new thread-pool task (following RestRequester.cs pattern)
            // Using GetAwaiter().GetResult() to preserve the original exception type
            Task.Run(async () => await AuthenticateCoreAsync(CancellationToken.None).ConfigureAwait(false)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Core async authentication logic shared by both sync and async methods.
        /// </summary>
        private async Task AuthenticateCoreAsync(CancellationToken cancellationToken)
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
            _urlValidator.ValidateTokenOrSsoUrl(ssoUrl, _oktaUrl);
            s_logger.Debug("Checking token URL");
            _urlValidator.ValidateTokenOrSsoUrl(tokenUrl, _oktaUrl);

            int retryCount = 0;
            int timeoutElapsed = 0;
            Exception lastRetryException = null;
            HttpResponseMessage samlRawResponse = null;

            // If postback URL verification fails, retry with new one-time token
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

        private void VerifyPostbackUrl()
        {
            Uri postBackUrl = _samlParser.ExtractPostbackUrl(_rawSamlTokenHtmlString);
            string sessionHost = session.properties[SFSessionProperty.HOST];
            string sessionScheme = session.properties[SFSessionProperty.SCHEME];
            _urlValidator.ValidatePostbackUrl(postBackUrl, sessionHost, sessionScheme);
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
}
