using System;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Security.Authentication;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests
{
    public class StorageTransportTest
    {
        [SFFact]
        public void TestHandlerCarriesRequestedTlsProtocols()
        {
            // arrange
            Skip.When(!CanRuntimeApplyTlsProtocols(), TlsProtocolsUnsupportedRationale);

            // act
            using var handler = Assert.IsType<HttpClientHandler>(
                HttpUtil.CreateStorageHandler(SslProtocolsExtensions.Tls13));

            // assert
            Assert.Equal(SslProtocolsExtensions.Tls13, handler.SslProtocols);
        }

        [SFFact]
        public void TestHandlerLeavesProtocolSelectionToOsWhenNothingRequested()
        {
            // arrange - the SslProtocols getter is unsupported on the same runtimes as the setter
            Skip.When(!CanRuntimeApplyTlsProtocols(), TlsProtocolsUnsupportedRationale);

            // act
            using var handler = Assert.IsType<HttpClientHandler>(
                HttpUtil.CreateStorageHandler(SslProtocols.None));

            // assert
            Assert.Equal(SslProtocols.None, handler.SslProtocols);
        }

        [SFFact]
        public void TestHandlerReappliesProxyBecauseSdkStopsReadingItFromConfig()
        {
            // arrange
            var proxyCredentials = new ProxyCredentials
            {
                ProxyHost = "proxy.snowflake.com",
                ProxyPort = 8080,
                ProxyUser = "proxyUser",
                ProxyPassword = "proxyPassword"
            };

            // act
            using var handler = Assert.IsType<HttpClientHandler>(
                HttpUtil.CreateStorageHandler(SslProtocols.None, proxyCredentials));

            // assert
            Assert.True(handler.UseProxy);
            var proxy = Assert.IsType<WebProxy>(handler.Proxy);
            Assert.Equal("proxy.snowflake.com", proxy.Address.Host);
            Assert.Equal(8080, proxy.Address.Port);
            Assert.NotNull(proxy.Credentials);
        }

        [SFFact]
        public void TestSharedHandlerIsReusedPerConfiguration()
        {
            // act
            using var firstClient = HttpUtil.Instance.CreateStorageHttpClientShared(SslProtocols.Tls12);
            using var secondClient = HttpUtil.Instance.CreateStorageHttpClientShared(SslProtocols.Tls12);
            using var otherClient = HttpUtil.Instance.CreateStorageHttpClientShared(SslProtocols.Tls12, allowAutoRedirect: true);

            // assert - one handler per configuration, so storage clients created per file do not churn sockets
            Assert.Same(HandlerOf(firstClient), HandlerOf(secondClient));
            Assert.NotSame(HandlerOf(firstClient), HandlerOf(otherClient));
        }

        [SFFact]
        public void TestDisposingAnIssuedClientLeavesTheSharedHandlerUsable()
        {
            // arrange - some SDK transports dispose the HttpClient they are handed
            var httpClient = HttpUtil.Instance.CreateStorageHttpClientShared(SslProtocols.Tls12, allowAutoRedirect: true, maxConnectionsPerServer: 7);
            var handlerBefore = HandlerOf(httpClient);

            // act
            httpClient.Dispose();

            // assert - the handler, and with it the connection pool, survives and is still handed out
            using var reissued = HttpUtil.Instance.CreateStorageHttpClientShared(SslProtocols.Tls12, allowAutoRedirect: true, maxConnectionsPerServer: 7);
            Assert.Same(handlerBefore, HandlerOf(reissued));
            Assert.Equal(System.Threading.Timeout.InfiniteTimeSpan, reissued.Timeout);
        }

        [SFFact]
        public void TestIssuedClientHasNoTimeoutSoLargeTransfersAreNotCutShort()
        {
            // act
            using var httpClient = HttpUtil.Instance.CreateStorageHttpClientShared(SslProtocols.Tls12);

            // assert
            Assert.Equal(System.Threading.Timeout.InfiniteTimeSpan, httpClient.Timeout);
        }

        [SFFact]
        public void TestCacheKeyNeverCarriesTheProxyPassword()
        {
            // arrange
            var proxyCredentials = new ProxyCredentials
            {
                ProxyHost = "proxy.snowflake.com",
                ProxyPort = 8080,
                ProxyUser = "proxyUser",
                ProxyPassword = "sup3rSecret"
            };

            // act
            var key = HttpUtil.BuildStorageHandlerKey(SslProtocols.Tls12, proxyCredentials, false, null);

            // assert - the key lives in a process-wide dictionary, so the secret must not be in it
            Assert.DoesNotContain("sup3rSecret", key);
            Assert.Contains("proxy.snowflake.com", key);
        }

        [SFFact]
        public void TestCacheKeyStillDistinguishesDifferentProxyPasswords()
        {
            // arrange - otherwise two proxies differing only by password would share a handler
            ProxyCredentials Credentials(string password) => new ProxyCredentials
            {
                ProxyHost = "proxy.snowflake.com",
                ProxyPort = 8080,
                ProxyUser = "proxyUser",
                ProxyPassword = password
            };

            // act
            var key = HttpUtil.BuildStorageHandlerKey(SslProtocols.Tls12, Credentials("one"), false, null);
            var otherKey = HttpUtil.BuildStorageHandlerKey(SslProtocols.Tls12, Credentials("two"), false, null);

            // assert
            Assert.NotEqual(otherKey, key);
        }

        [SFTheory]
        [InlineData(false)]
        [InlineData(true)]
        public void TestCacheKeyDistinguishesRedirectBehavior(bool allowAutoRedirect)
        {
            // act
            var key = HttpUtil.BuildStorageHandlerKey(SslProtocols.Tls12, null, allowAutoRedirect, null);
            var otherKey = HttpUtil.BuildStorageHandlerKey(SslProtocols.Tls12, null, !allowAutoRedirect, null);

            // assert
            Assert.NotEqual(otherKey, key);
        }

        private const string TlsProtocolsUnsupportedRationale =
            "This runtime does not support setting TLS protocols on HttpClientHandler (.NET Framework 4.6.2 and 4.7.1).";

        private static bool CanRuntimeApplyTlsProtocols()
        {
            try
            {
                using var handler = new HttpClientHandler();
                handler.SslProtocols = SslProtocols.Tls12 | SslProtocolsExtensions.Tls13;
                return true;
            }
            catch (PlatformNotSupportedException)
            {
                return false;
            }
        }

        private static HttpClientHandler HandlerOf(HttpClient client)
        {
            var field = typeof(HttpMessageInvoker).GetField("_handler", BindingFlags.Instance | BindingFlags.NonPublic)
                        ?? typeof(HttpMessageInvoker).GetField("handler", BindingFlags.Instance | BindingFlags.NonPublic);
            return Assert.IsType<HttpClientHandler>(field.GetValue(client));
        }
    }
}
