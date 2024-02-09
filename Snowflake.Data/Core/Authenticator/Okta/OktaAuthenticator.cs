/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.Okta.Models;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    /// <summary>
    /// OktaAuthenticator would perform several steps of authentication with Snowflake and Okta idp
    /// </summary>
    internal class OktaAuthenticator : BaseAuthenticator, IAuthenticator
    {
        private readonly ISamlRestRequestFactory _samlRestRequestFactory;
        private readonly IIdpTokenRestRequestFactory _idpTokenRestRequestFactory;
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OktaAuthenticator>();

        /// <summary>
        /// url of the okta idp
        /// </summary>
        private readonly Uri _oktaUrl;

        // The raw Saml token.
        private string _samlRawHtmlString;

        /// <summary>
        /// Constructor of the Okta authenticator
        /// </summary>
        /// <param name="session"></param>
        /// <param name="oktaUriString"></param>
        /// <param name="samlRestRequestFactory"></param>
        /// <param name="idpTokenRestRequestFactory"></param>
        internal OktaAuthenticator(SFSession session, string oktaUriString, ISamlRestRequestFactory samlRestRequestFactory, IIdpTokenRestRequestFactory idpTokenRestRequestFactory) : 
            base(session, oktaUriString)
        {
            _samlRestRequestFactory = samlRestRequestFactory;
            _idpTokenRestRequestFactory = idpTokenRestRequestFactory;
            _oktaUrl = new Uri(oktaUriString);
        }

        /// <see cref="IAuthenticator"/>
        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            s_logger.Info("Okta Authentication");

            s_logger.Debug("step 1: get sso and token url");
            var authenticatorRestRequest = BuildAuthenticatorRestRequest();
            var authenticatorResponse = await Session.restRequester.PostAsync<AuthenticatorResponse>(authenticatorRestRequest, cancellationToken);
            authenticatorResponse.FilterFailedResponse();
            var ssoUrl = new Uri(authenticatorResponse.data.ssoUrl);
            var tokenUrl = new Uri(authenticatorResponse.data.tokenUrl);

            s_logger.Debug("step 2: verify urls fetched from step 1");
            s_logger.Debug("Checking sso url");
            VerifyUrls(ssoUrl, _oktaUrl);
            s_logger.Debug("Checking token url");
            VerifyUrls(tokenUrl, _oktaUrl);

            s_logger.Debug("step 3: get idp onetime token");
            var idpTokenRestRequest = _idpTokenRestRequestFactory.Create(tokenUrl, Session);
            var idpResponse = await Session.restRequester.PostAsync<IdpTokenResponse>(idpTokenRestRequest, cancellationToken);
            var onetimeToken = idpResponse.SessionToken ?? idpResponse.CookieToken;

            s_logger.Debug("step 4: get SAML response from sso");
            var samlRestRequest = _samlRestRequestFactory.Create(ssoUrl, onetimeToken, Session.connectionTimeout);
            using (var samlRawResponse = await Session.restRequester.GetAsync(samlRestRequest, cancellationToken))
            { 
                _samlRawHtmlString = await samlRawResponse.Content.ReadAsStringAsync(cancellationToken);
            }

            s_logger.Debug("step 5: verify postback url in SAML response");
            VerifyPostbackUrl();

            s_logger.Debug("step 6: send SAML response to snowflake to login");
            await LoginAsync(cancellationToken);
        }

        void IAuthenticator.Authenticate()
        {
            AuthenticateAsync(CancellationToken.None).Wait();
        }

        private SFRestRequest BuildAuthenticatorRestRequest()
        {
            var fedUrl = Session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);
            var data = new AuthenticatorRequestData()
            {
                AccountName = Session.properties[SFSessionProperty.ACCOUNT],
                Authenticator = _oktaUrl.ToString(),
                DriverVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString(),
                DriverName = ".NET"
            };

            return Session.BuildTimeoutRestRequest(fedUrl, new AuthenticatorRequest() { Data = data });
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.RawSamlResponse = _samlRawHtmlString;
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
            var formIndex = _samlRawHtmlString.IndexOf("<form");
            // skip 'action="' (length = 8)
            var startIndex = _samlRawHtmlString.IndexOf("action=", formIndex) + 8;
            var length = _samlRawHtmlString.IndexOf('"', startIndex) - startIndex;

            Uri postBackUrl;
            try
            {
                postBackUrl = new Uri(HttpUtility.HtmlDecode(_samlRawHtmlString.Substring(startIndex, length)));
            } catch (Exception e)
            {
                s_logger.Error("Fail to extract SAML from html", e);
                throw new SnowflakeDbException(e, SFError.IDP_SAML_POSTBACK_NOTFOUND);
            }

            var sessionHost = Session.properties[SFSessionProperty.HOST];
            var sessionScheme = Session.properties[SFSessionProperty.SCHEME];
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
    }
}
