using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using System;
    using Snowflake.Data.Core;
    public class SFUriUpdaterTest
    {
        [SFFact]
        public void TestRetryCount()
        {
            Uri uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_QUERY_PATH);

            HttpUtil.UriUpdater updater = new HttpUtil.UriUpdater(uri);

            for (int retryCount = 1; retryCount < 5; retryCount++)
            {
                Uri newUri = updater.Update();

                Assert.Contains(RestParams.SF_QUERY_RETRY_COUNT + "=" + retryCount, newUri.Query);
            }
        }

        [SFFact]
        public void TestRetryReasonEnabled()
        {
            Uri uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_QUERY_PATH);

            HttpUtil.UriUpdater updater = new HttpUtil.UriUpdater(uri, true);

            Uri newUri = updater.Update(429);

            Assert.Contains(RestParams.SF_QUERY_RETRY_REASON + "=" + 429, newUri.Query);
        }

        [SFFact]
        public void TestRetryReasonDisabled()
        {
            Uri uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_QUERY_PATH);

            HttpUtil.UriUpdater updater = new HttpUtil.UriUpdater(uri, false);

            Uri newUri = updater.Update(429);

            Assert.DoesNotContain(RestParams.SF_QUERY_RETRY_REASON, newUri.Query);
        }

        [SFFact]
        /// This uri with query path other than query request should not have a retry counter
        public void TestRetryCountNoneQueryPath()
        {
            Uri uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);

            HttpUtil.UriUpdater updater = new HttpUtil.UriUpdater(uri);

            Uri newUri = updater.Update();

            Assert.DoesNotContain(RestParams.SF_QUERY_RETRY_COUNT, newUri.Query);
        }

        [SFFact]
        public void TestRequestGUIDUpdate()
        {
            Uri uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH);
            HttpUtil.UriUpdater updater = new HttpUtil.UriUpdater(uri);

            // A uri with no request_guid at the begining should not change with the updater.
            Uri newUri = updater.Update();

            Assert.Equal(newUri.ToString(), uri.ToString());

            // A uri with request_guid should update that param
            string initialGuid = Guid.NewGuid().ToString();
            uri = new Uri("https://ac.snowflakecomputing.com" + RestPath.SF_LOGIN_PATH
                + "?" + RestParams.SF_QUERY_REQUEST_GUID + "=" + initialGuid);

            updater = new HttpUtil.UriUpdater(uri);
            newUri = updater.Update();

            Assert.Contains(RestParams.SF_QUERY_REQUEST_GUID, newUri.Query);
            Assert.DoesNotContain(initialGuid, newUri.Query);
            Assert.Equal(newUri.ToString().Length, uri.ToString().Length);

        }
    }
}
