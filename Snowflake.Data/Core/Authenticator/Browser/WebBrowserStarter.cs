using System;
using System.Text.RegularExpressions;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class WebBrowserStarter
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WebBrowserStarter>();

        private readonly IWebBrowserRunner _runner;

        public static WebBrowserStarter Instance = new WebBrowserStarter(WebBrowserRunner.Instance);

        internal WebBrowserStarter(IWebBrowserRunner runner)
        {
            _runner = runner;
        }

        public void StartBrowser(string url)
        {
            string regexStr = "^http(s?)\\:\\/\\/[0-9a-zA-Z]([-.\\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\\/?)([a-zA-Z0-9\\-\\.\\?\\,\\&\\(\\)\\/\\\\\\+&%\\$#_=@]*)?$";
            Match m = Regex.Match(url, regexStr, RegexOptions.IgnoreCase);
            if (!m.Success || !Uri.IsWellFormedUriString(url, UriKind.Absolute))
            {
                ThrowInvalidBrowserUrlException();
            }
            var uri = new Uri(url);
            if (url != uri.ToString())
            {
                if (url.StartsWith("http://localhost:1080/oauth/authorize?client_id=123"))
                {
                    s_logger.Warn($"!!!!!! url: {url}");
                    s_logger.Warn($"!!!!!! uri.ToString(): {uri}");
                }
                ThrowInvalidBrowserUrlException();
            }
            _runner.Run(uri);
        }

        private void ThrowInvalidBrowserUrlException()
        {
            s_logger.Error("Failed to start browser. Invalid url.");
            throw new SnowflakeDbException(SFError.INVALID_BROWSER_URL, "****");
        }
    }
}
