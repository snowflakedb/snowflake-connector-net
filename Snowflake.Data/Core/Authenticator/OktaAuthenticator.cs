/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
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
using AngleSharp;

namespace Snowflake.Data.Core.Authenticator
{
    class OktaAuthenticator : IAuthenticator
    {
        private const SFAuthenticatorType TYPE = SFAuthenticatorType.OKTA;
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<OktaAuthenticator>();
        private SFSession session;
        private Uri oktaUrl;

        internal OktaAuthenticator(SFSession session, string oktaUriString)
        {
            this.session = session;
            oktaUrl = new Uri(oktaUriString);
        }

        void IAuthenticator.Authenticate()
        {
            // step1l
            var requestStep1 = BuildFedRequest();
            var responseStep1 = session.restRequester.Post<AuthnResponse>(requestStep1);
            FilterFailedResponse(responseStep1);
            Uri ssoUrl = new Uri(responseStep1.data.ssoUrl);
            Uri tokenUrl = new Uri(responseStep1.data.tokenUrl);

            // step2
            VerifyUrls(ssoUrl, tokenUrl);

            // step3
            var idpRestRequest = new IdpTokenRestRequest()
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
            var idpResponse = session.restRequester.Post<IdpTokenResponse>(idpRestRequest);
            string oneTimeToken = idpResponse.CookieToken;

            // step 4
            var samlRestRequest = new SAMLRestRequest()
            {
                Url = ssoUrl,
                RestTimeout = session.connectionTimeout,
                HttpTimeout = Timeout.InfiniteTimeSpan,
            };
            var samlRawResponse = Task.Run(async () => await session.restRequester.GetAsync(samlRestRequest, CancellationToken.None)).Result;
            var samlRawContents = Task.Run(async () => await samlRawResponse.Content.ReadAsStreamAsync()).Result;
            using (var document = Task.Run(async () => await BrowsingContext.New(AngleSharp.Configuration.Default).OpenAsync(req => req.Content(samlRawContents))))
            {
                // TODO step 5 verify the postback url
            }

            //step 6
            var fedUrl = session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);


            
            


        }


        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {

        }

        private SFRestRequest BuildFedRequest()
        {
            var fedUrl = session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);
            var data = new AuthnRequestData()
            {
                accountName = session.properties[SFSessionProperty.ACCOUNT],
                Authenticator = TYPE.ToString(),
                clientAppId = ".NET",
                clientAppVersion = SFEnvironment.Version,
                clientEnv = SFEnvironment.ClientEnv,
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(fedUrl, new AuthnRequest() { data = data });
        }

        private void VerifyUrls(Uri ssoUrl, Uri tokenUrl)
        {

            if (ssoUrl.Scheme != oktaUrl.Scheme || ssoUrl.Host != oktaUrl.Host)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_INCORRECT_DESTINATION,
                    new object[] { "sso url : " + ssoUrl.ToString(), "okta url: " + oktaUrl.ToString() });
                logger.Error("Different sso url than oktaUrl", e);
                throw e;
            }

            if (tokenUrl.Scheme != oktaUrl.Scheme || tokenUrl.Host != oktaUrl.Host)
            {
                var e = new SnowflakeDbException(
                    SFError.IDP_INCORRECT_DESTINATION,
                    new object[] { "token url : " + tokenUrl.ToString(), "okta url: " + oktaUrl.ToString() });
                logger.Error("Different token url than oktaUrl", e);
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
    }

    class IdpTokenRestRequest : BaseRestRequest, IRestRequest
    {   
        private static MediaTypeWithQualityHeaderValue jsonHeader = new MediaTypeWithQualityHeaderValue("application/json");

        internal IdpTokenRequest JsonBody { get; set; }

        HttpRequestMessage ToRequestMessage(HttpMethod method)
        {
            HttpRequestMessage message = newMessage(method, Url);
            message.Headers.Accept.Add(jsonHeader);

            var json = JsonConvert.SerializeObject(JsonBody);
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
    }

    class SAMLRestRequest : BaseRestRequest, IRestRequest
    {
        internal string OnetimeToken { set; get; }
        HttpRequestMessage ToRequestMessage(HttpMethod method)
        {
            UriBuilder builder = new UriBuilder(Url);
            builder.Query = "RelayState=%2Fsome%2Fdeep%2Flink&onetimetoken&" + OnetimeToken;
            HttpRequestMessage message = newMessage(method, builder.Uri);

            message.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));

            return message;
        }
    }


}
