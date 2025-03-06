using System;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal interface IBrowserError
    {
        string GetBrowserError();
        Exception GetException();
    }
}
