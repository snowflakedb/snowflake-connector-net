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
            Match urlMatch = Regex.Match(url, regexStr, RegexOptions.IgnoreCase);
            if (!urlMatch.Success || !Uri.IsWellFormedUriString(url, UriKind.Absolute))
            {
                ThrowInvalidBrowserUrlException();
            }
            var uri = new Uri(url);
            var uriMatch = Regex.Match(uri.ToString(), regexStr, RegexOptions.IgnoreCase);
            if (!uriMatch.Success || !Uri.IsWellFormedUriString(uri.ToString(), UriKind.Absolute))
            {
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
