using System;
using System.Text.RegularExpressions;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class WebBrowserStarter
    {
        private const string UrlRegexString = "^http(s?)\\:\\/\\/[0-9a-zA-Z]([-.\\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\\/?)([a-zA-Z0-9\\-\\.\\?\\,\\&\\(\\)\\/\\\\\\+&%\\$#_=@]*)?$";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WebBrowserStarter>();

        private readonly IWebBrowserRunner _runner;

        public static WebBrowserStarter Instance = new WebBrowserStarter(WebBrowserRunner.Instance);

        internal WebBrowserStarter(IWebBrowserRunner runner)
        {
            _runner = runner;
        }

        public void StartBrowser(Url url)
        {
            ValidateUrl(url);
            var uri = new Uri(url.Value);
            _runner.Run(uri);
        }

        private void ValidateUrl(Url url)
        {
            Match urlMatch = Regex.Match(url.Value, UrlRegexString, RegexOptions.IgnoreCase);
            if (!urlMatch.Success || !Uri.IsWellFormedUriString(url.Value, UriKind.Absolute))
            {
                s_logger.Error("Failed to start browser. Invalid url.");
                throw new SnowflakeDbException(SFError.INVALID_BROWSER_URL, url.ValueWithoutSecrets);
            }
        }
    }
}
