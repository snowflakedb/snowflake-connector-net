using System;
using Snowflake.Data.Core.Authenticator.Okta.Models;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal interface ISamlRestRequestFactory
    {
        SamlRestRequest Create(Uri ssoUrl, string onetimeToken, TimeSpan timeout);
    }
}