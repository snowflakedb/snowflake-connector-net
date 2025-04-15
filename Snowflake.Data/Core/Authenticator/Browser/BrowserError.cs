using System;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class BrowserError : IBrowserError
    {
        public string BrowserMessage { get; set; }

        public SnowflakeDbException Exception { get; set; }

        public string GetBrowserError()
        {
            return BrowserMessage;
        }

        public Exception GetException()
        {
            return Exception;
        }
    }
}
