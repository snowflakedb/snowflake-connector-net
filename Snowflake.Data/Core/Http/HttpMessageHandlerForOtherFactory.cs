/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Net;
using System.Net.Http;
using System.Security.Authentication;

namespace Snowflake.Data.Core
{

    internal class HttpMessageHandlerForOtherFactory : HttpMessageHandlerFactory
    {
        protected override HttpMessageHandler CreateHandlerWithoutProxy(HttpClientConfig config)
        {
            return new HttpClientHandler()
            {
                // Verify no certificates have been revoked
                CheckCertificateRevocationList = config.CrlCheckEnabled,
                // Enforce tls v1.2
                SslProtocols = SslProtocols.Tls12,
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false // Disable cookies
            };
        }

        protected override HttpMessageHandler AttachProxyToHandler(HttpMessageHandler httpMessageHandler,
            WebProxy proxy)
        {
            HttpClientHandler httpHandlerWithProxy = (HttpClientHandler)httpMessageHandler;
            httpHandlerWithProxy.Proxy = proxy;
            return httpHandlerWithProxy;
        }

        public override IWebProxy ExtractWebProxy(HttpMessageHandler httpMessageHandler)
        {
            return ((HttpClientHandler)httpMessageHandler).Proxy;
        }
    }
}
