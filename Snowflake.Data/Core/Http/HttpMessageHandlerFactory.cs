/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Net;
using System.Net.Http;

namespace Snowflake.Data.Core
{

    internal abstract class HttpMessageHandlerFactory
    {

        public HttpMessageHandler Create(HttpClientConfig config)
        {
            var handler = CreateHandlerWithoutProxy(config);
            if (ProxyNeedsToBeAdded(config))
            {
                var proxy = CreateProxy(config);
                return AttachProxyToHandler(handler, proxy);
            }

            return handler;
        }

        protected abstract HttpMessageHandler CreateHandlerWithoutProxy(HttpClientConfig config);

        protected abstract HttpMessageHandler AttachProxyToHandler(HttpMessageHandler httpMessageHandler,
            WebProxy proxy);

        public abstract IWebProxy ExtractWebProxy(HttpMessageHandler httpMessageHandler);

        private WebProxy CreateProxy(HttpClientConfig config)
        {
            WebProxy proxy = new WebProxy(config.ProxyHost, int.Parse(config.ProxyPort));
            AttachCredentials(proxy, config);
            AttachBypassList(proxy, config);
            return proxy;
        }

        private bool ProxyNeedsToBeAdded(HttpClientConfig config)
        {
            return config.ProxyHost != null;
        }

        private WebProxy AttachCredentials(WebProxy proxy, HttpClientConfig config)
        {
            if (String.IsNullOrEmpty(config.ProxyUser))
                return proxy;
            ICredentials credentials = new NetworkCredential(config.ProxyUser, config.ProxyPassword);
            proxy.Credentials = credentials;
            return proxy;
        }

        private WebProxy AttachBypassList(WebProxy proxy, HttpClientConfig config)
        {
            if (String.IsNullOrEmpty(config.NoProxyList))
                return proxy;
            string[] bypassList = config.NoProxyList.Split(
                new char[] { '|' },
                StringSplitOptions.RemoveEmptyEntries);
            // Convert simplified syntax to standard regular expression syntax
            string entry = null;
            for (int i = 0; i < bypassList.Length; i++)
            {
                // Get the original entry
                entry = bypassList[i].Trim();
                // . -> [.] because . means any char 
                entry = entry.Replace(".", "[.]");
                // * -> .*  because * is a quantifier and need a char or group to apply to
                entry = entry.Replace("*", ".*");

                // Replace with the valid entry syntax
                bypassList[i] = entry;
            }

            proxy.BypassList = bypassList;
            return proxy;
        }
    }
}
