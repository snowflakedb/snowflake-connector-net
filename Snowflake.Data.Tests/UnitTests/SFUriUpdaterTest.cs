using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class SFUriUpdaterTest
    {
        [SFFact]
        public void TestRetryCount()
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_QUERY_PATH);
            var updater = new HttpUtil.UriUpdater(uri);

            for (int retryCount = 1; retryCount < 5; retryCount++)
            {
                var request = new HttpRequestMessage(HttpMethod.Post, uri);
                updater.Update(ref request, default, null);

                Assert.Contains(RestParams.SF_QUERY_RETRY_COUNT + "=" + retryCount, request.RequestUri.Query);
            }
        }

        [SFFact]
        public void TestRetryReasonEnabled()
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_QUERY_PATH);
            var updater = new HttpUtil.UriUpdater(uri, true);

            var request = new HttpRequestMessage(HttpMethod.Post, uri);
            updater.Update(ref request, (HttpStatusCode)429, null);

            Assert.Contains(RestParams.SF_QUERY_RETRY_REASON + "=" + 429, request.RequestUri.Query);
        }

        [SFFact]
        public void TestRetryReasonDisabled()
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_QUERY_PATH);
            var updater = new HttpUtil.UriUpdater(uri, false);

            var request = new HttpRequestMessage(HttpMethod.Post, uri);
            updater.Update(ref request, (HttpStatusCode)429, null);

            Assert.DoesNotContain(RestParams.SF_QUERY_RETRY_REASON, request.RequestUri.Query);
        }

        [SFFact]
        /// This uri with query path other than query request should not have a retry counter
        public void TestRetryCountNoneQueryPath()
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);
            var updater = new HttpUtil.UriUpdater(uri);

            var request = new HttpRequestMessage(HttpMethod.Post, uri);
            updater.Update(ref request, default, null);

            Assert.DoesNotContain(RestParams.SF_QUERY_RETRY_COUNT, request.RequestUri.Query);
        }

        [SFFact]
        public void TestRequestGUIDUpdate()
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);
            var updater = new HttpUtil.UriUpdater(uri);

            // A uri with no request_guid at the beginning should not change with the updater.
            var request = new HttpRequestMessage(HttpMethod.Post, uri);
            updater.Update(ref request, default, null);

            Assert.Equal(uri.ToString(), request.RequestUri.ToString());

            // A uri with request_guid should update that param
            string initialGuid = Guid.NewGuid().ToString();
            uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH
                + "?" + RestParams.SF_QUERY_REQUEST_GUID + "=" + initialGuid);

            updater = new HttpUtil.UriUpdater(uri);
            request = new HttpRequestMessage(HttpMethod.Post, uri);
            updater.Update(ref request, default, null);

            Assert.Contains(RestParams.SF_QUERY_REQUEST_GUID, request.RequestUri.Query);
            Assert.DoesNotContain(initialGuid, request.RequestUri.Query);
            Assert.Equal(request.RequestUri.ToString().Length, uri.ToString().Length);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.Moved)]        // 301
        [InlineData(HttpStatusCode.Redirect)]      // 302
        [InlineData(HttpStatusCode.SeeOther)]      // 303
        public async Task TestRedirectDowngradesPostToGetAndDisposesContent(HttpStatusCode statusCode)
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);
            var updater = new HttpUtil.UriUpdater(uri);
            var content = new StringContent("{\"data\":{}}");

            var request = new HttpRequestMessage(HttpMethod.Post, uri) { Content = content };
            updater.Update(ref request, statusCode, new Uri("/redirected", UriKind.Relative));

            Assert.Equal(HttpMethod.Get, request.Method);
            // Content must be disposed when downgrading POST to GET — the body is not
            // valid for the new method and would leak if kept alive
            await Assert.ThrowsAsync<ObjectDisposedException>(() => content.ReadAsStringAsync()).ConfigureAwait(false);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.TemporaryRedirect)]  // 307
        [InlineData((HttpStatusCode)308)]               // 308
        public void TestRedirectPreservesMethodAndContentFor307And308(HttpStatusCode statusCode)
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);
            var updater = new HttpUtil.UriUpdater(uri);
            var content = new StringContent("{\"data\":{}}");

            var request = new HttpRequestMessage(HttpMethod.Post, uri) { Content = content };
            updater.Update(ref request, statusCode, new Uri("/redirected", UriKind.Relative));

            Assert.Equal(HttpMethod.Post, request.Method);
            // Content must NOT be disposed for 307/308 — the body is replayed as-is
            Assert.Same(content, request.Content);
        }

        [SFFact]
        public void TestRedirectUpdatesRequestUri()
        {
            var uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);
            var updater = new HttpUtil.UriUpdater(uri);

            var request = new HttpRequestMessage(HttpMethod.Post, uri);
            updater.Update(ref request, HttpStatusCode.TemporaryRedirect, new Uri("/new-path", UriKind.Relative));

            Assert.Equal("/new-path", request.RequestUri.AbsolutePath);
            Assert.Equal("ac.snowflakecomputing.com", request.RequestUri.Host);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.Ambiguous)]     // 300
        [InlineData(HttpStatusCode.Moved)]         // 301
        [InlineData(HttpStatusCode.Redirect)]      // 302
        [InlineData(HttpStatusCode.SeeOther)]      // 303
        [InlineData(HttpStatusCode.TemporaryRedirect)]  // 307
        [InlineData((HttpStatusCode)308)]               // 308
        public void TestIsRedirectHTTPCode(HttpStatusCode statusCode)
        {
            Assert.True(HttpUtil.IsRedirectHTTPCode(statusCode));
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK)]
        [InlineData(HttpStatusCode.NotFound)]
        [InlineData(HttpStatusCode.InternalServerError)]
        [InlineData(HttpStatusCode.Forbidden)]
        [InlineData((HttpStatusCode)429)]
        public void TestIsNotRedirectHTTPCode(HttpStatusCode statusCode)
        {
            Assert.False(HttpUtil.IsRedirectHTTPCode(statusCode));
        }
    }
}
