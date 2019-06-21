﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// ExternalBrowserAuthenticator would start a new browser to perform authentication
    /// </summary>
    class ExternalBrowserAuthenticator : IAuthenticator
    {
        public static readonly string AUTH_NAME = "externalbrowser";
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<ExternalBrowserAuthenticator>();
        private static readonly string TOKEN_REQUEST_PREFIX = "?token=";
        private static readonly byte[] SUCCESS_RESPONSE = System.Text.Encoding.UTF8.GetBytes(
            "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
            "<title> SAML Response for Snowflake </title></head>" +
            "<body>Your identity was confirmed and propagated to Snowflake .NET driver. You can close this window now and go back where you started from." +
            "</body></html>;"
            );
        /// <summary>
        /// Session that create this authenticator
        /// </summary>
        private SFSession session;


        /// <summary>
        /// Constructor of the External authenticator
        /// </summary>
        /// <param name="session"></param>
        internal ExternalBrowserAuthenticator(SFSession session)
        {
            this.session = session;
        }

        /// <see cref="IAuthenticator"/>
        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            logger.Info("External Browser Authentication");

            int localPort = GetRandomUnusedPort();
            string proofKey;
            string samlResponseToken;
            using (var httpListener = GetHttpListener(localPort))
            {
                httpListener.Start();

                logger.Debug("Get IdpUrl and ProofKey");
                var authenticatorRestRequest = BuildAuthenticatorRestRequest(localPort);
                var authenticatorRestResponse =
                    await session.restRequester.PostAsync<AuthenticatorResponse>(
                        authenticatorRestRequest,
                        cancellationToken
                    );
                authenticatorRestResponse.FilterFailedResponse();

                var idpUrl = authenticatorRestResponse.data.ssoUrl;
                proofKey = authenticatorRestResponse.data.proofKey;

                logger.Debug("Open browser");
                StartBrowser(idpUrl);

                logger.Debug("Get the redirect SAML request");
                var context = await httpListener.GetContextAsync();
                var request = context.Request;
                samlResponseToken = ValidateAndExtractToken(request);
                HttpListenerResponse response = context.Response;
                try
                {
                    using (var output = response.OutputStream)
                    {
                        await output.WriteAsync(SUCCESS_RESPONSE, 0, SUCCESS_RESPONSE.Length);
                    }
                }
                catch (Exception e)
                {
                    // Ignore the exception as it does not affect the overall authentication flow
                }

                httpListener.Stop();
            }

            logger.Debug("Send login request");
            var loginResponse = await session.restRequester.PostAsync<LoginResponse>(
                BuildExternalBrowserLoginRequest(samlResponseToken, proofKey),
                cancellationToken
                );
            session.ProcessLoginResponse(loginResponse);
        }

        /// <see cref="IAuthenticator"/>
        void IAuthenticator.Authenticate()
        {
            logger.Info("External Browser Authentication");

            int localPort = GetRandomUnusedPort();
            string proofKey;
            string samlResponseToken;
            using (var httpListener = GetHttpListener(localPort))
            {
                httpListener.Start();

                logger.Debug("Get IdpUrl and ProofKey");
                var authenticatorRestRequest = BuildAuthenticatorRestRequest(localPort);
                var authenticatorRestResponse = session.restRequester.Post<AuthenticatorResponse>(authenticatorRestRequest);
                authenticatorRestResponse.FilterFailedResponse();

                var idpUrl = authenticatorRestResponse.data.ssoUrl;
                proofKey = authenticatorRestResponse.data.proofKey;

                logger.Debug("Open browser");
                StartBrowser(idpUrl);

                logger.Debug("Get the redirect SAML request");
                var context = httpListener.GetContext();
                var request = context.Request;
                samlResponseToken = ValidateAndExtractToken(request);
                HttpListenerResponse response = context.Response;
                try
                {
                    using (var output = response.OutputStream)
                    {
                        output.Write(SUCCESS_RESPONSE, 0, SUCCESS_RESPONSE.Length);
                    }
                }
                catch (Exception e)
                {
                    // Ignore the exception as it does not affect the overall authentication flow
                }

                httpListener.Stop();
            }

            logger.Debug("Send login request");
            var loginResponse = session.restRequester.Post<LoginResponse>(
                BuildExternalBrowserLoginRequest(samlResponseToken, proofKey)
                );
            session.ProcessLoginResponse(loginResponse);
        }

        private static int GetRandomUnusedPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        private static HttpListener GetHttpListener(int port)
        {
            string redirectURI = string.Format("http://{0}:{1}/", IPAddress.Loopback, port);
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add(redirectURI);
            return listener;
        }

        private static void StartBrowser(string url)
        {
            // The following code is learnt from https://brockallen.com/2016/09/24/process-start-for-urls-on-net-core/
#if NET46
            // .net standard would pass here
            Process.Start(url);
#else
            // hack because of this: https://github.com/dotnet/corefx/issues/10361
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                url = url.Replace("&", "^&");
                Process.Start(new ProcessStartInfo("cmd", $"/c start {url}") { CreateNoWindow = true });
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Process.Start("xdg-open", url);
            }
            else 
            {
                Process.Start("open", url);
            }
#endif
        }

        private static string ValidateAndExtractToken(HttpListenerRequest request)
        {
            if (request.HttpMethod != "GET")
            {
                throw new SnowflakeDbException(SFError.BROWSER_RESPONSE_WRONG_METHOD, request.HttpMethod);
            }

            if (request.Url.Query == null || !request.Url.Query.StartsWith(TOKEN_REQUEST_PREFIX))
            {
                throw new SnowflakeDbException(SFError.BROWSER_RESPONSE_INVALID_PREFIX, request.Url.Query);
            }

            return Uri.UnescapeDataString(request.Url.Query.Substring(TOKEN_REQUEST_PREFIX.Length));
        }

        private SFRestRequest BuildAuthenticatorRestRequest(int port)
        {
            var fedUrl = session.BuildUri(RestPath.SF_AUTHENTICATOR_REQUEST_PATH);
            var data = new AuthenticatorRequestData()
            {
                AccountName = session.properties[SFSessionProperty.ACCOUNT],
                Authenticator = AUTH_NAME,
                BrowserModeRedirectPort = port.ToString(),
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(fedUrl, new AuthenticatorRequest() { Data = data });
        }

        private SFRestRequest BuildExternalBrowserLoginRequest(string token, string proofKey)
        {
            // build uri
            var loginUrl = session.BuildLoginUrl();

            LoginRequestData data = new LoginRequestData()
            {
                clientAppId = ".NET",
                clientAppVersion = SFEnvironment.Version,
                clientEnv = SFEnvironment.ClientEnv,
                Token = token,
                ProofKey = proofKey,
                Authenticator = AUTH_NAME,
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(loginUrl, new LoginRequest() { data = data });
        }
    }
}
