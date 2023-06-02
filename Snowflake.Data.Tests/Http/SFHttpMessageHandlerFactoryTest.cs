/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Net.Http;
using Snowflake.Data.Core;

using NUnit.Framework;

namespace Snowflake.Data.Tests
{
    [TestFixture]
    public class SFHttpMessageHandlerFactoryTest
    {

        [Test]
        public void ShouldCreateHttpMessageHandlerWithoutProxyTest()
        {
            // given
            var config = new HttpClientConfig(
                crlCheckEnabled: true,
                proxyHost: null,
                proxyPort: null,
                proxyUser: null,
                proxyPassword: null,
                noProxyList: null,
                disableRetry: false,
                forceRetryOn404: false
            );
            var handlerFactory = new HttpMessageHandlerFactoryProvider().createHttpMessageHandlerFactory();

            // when
            var handler = (HttpClientHandler)handlerFactory.Create(config);

            // then
            Assert.NotNull(handler);
            Assert.Null(handlerFactory.ExtractWebProxy(handler));
        }

        [Test]
        public void ShouldCreateHttpMessageHandlerWithProxyTest()
        {
            // given
            var config = new HttpClientConfig(
                crlCheckEnabled: true,
                proxyHost: "proxy.host.com",
                proxyPort: "1234",
                proxyUser: "user",
                proxyPassword: "password",
                noProxyList: null,
                disableRetry: false,
                forceRetryOn404: false
            );
            var handlerFactory = new HttpMessageHandlerFactoryProvider().createHttpMessageHandlerFactory();

            // when
            var handler = (HttpClientHandler)handlerFactory.Create(config);

            // then
            Assert.NotNull(handler);
            Assert.NotNull(handlerFactory.ExtractWebProxy(handler));
        }
    }
}
