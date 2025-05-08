using System;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal interface IWebBrowserRunner
    {
        void Run(Uri uri);
    }
}
