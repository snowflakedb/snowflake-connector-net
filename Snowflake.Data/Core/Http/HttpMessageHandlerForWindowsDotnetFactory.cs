/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Net;
using System.Net.Http;
using System.Security.Authentication;

namespace Snowflake.Data.Core;

internal class HttpMessageHandlerForWindowsDotnetFactory: HttpMessageHandlerFactory
{
    protected override HttpMessageHandler CreateHandlerWithoutProxy(HttpClientConfig config)
    {
        return new WinHttpHandler()
        {
            // Verify no certificates have been revoked
            CheckCertificateRevocationList = config.CrlCheckEnabled,
            // Enforce tls v1.2
            SslProtocols = SslProtocols.Tls12,
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            CookieUsePolicy = CookieUsePolicy.IgnoreCookies
        };
    }

    protected override HttpMessageHandler AttachProxyToHandler(HttpMessageHandler httpMessageHandler, WebProxy proxy)
    {
        WinHttpHandler httpHandlerWithProxy = (WinHttpHandler) httpMessageHandler;
        httpHandlerWithProxy.WindowsProxyUsePolicy = WindowsProxyUsePolicy.UseCustomProxy;
        httpHandlerWithProxy.Proxy = proxy;
        return httpHandlerWithProxy;
    }

    public override IWebProxy ExtractWebProxy(HttpMessageHandler httpMessageHandler)
    {
        return ((WinHttpHandler) httpMessageHandler).Proxy;
    }
}