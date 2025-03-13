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
            if (!m.Success)
            {
                ThrowInvalidBrowserUrlException();
            }
            if (!Uri.IsWellFormedUriString(url, UriKind.Absolute))
            {
                ThrowInvalidBrowserUrlException();
            }
            _runner.Run(url);
        }

        private void ThrowInvalidBrowserUrlException()
        {
            var errorMessage = "Failed to start browser. Invalid url.";
            s_logger.Error(errorMessage);
            throw new SnowflakeDbException(SFError.INVALID_BROWSER_URL, errorMessage);
        }
    }
}
