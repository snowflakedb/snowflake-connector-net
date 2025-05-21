using System;
using System.Net.Http;
using Snowflake.Data.Core.Authenticator.Browser;

namespace Snowflake.Data.Tests.Mock
{
    internal class MockBrowser : IWebBrowserRunner
    {
        private HttpClient _httpClient;

        public MockBrowser()
        {
            _httpClient = new HttpClient();
        }

        public void Run(Uri uri)
        {
            _httpClient.GetAsync(uri.ToString()); // we should not wait for the response because in case of 302 Redirect it will be sent to a different endpoint so we won't get a response here
        }
    }
}
