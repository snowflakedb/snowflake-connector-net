using System;
using System.Threading;
using Snowflake.Data.Core.Authenticator.Okta.Models;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal class SamlRestRequestFactory : ISamlRestRequestFactory
    {
        public SamlRestRequest Create(Uri ssoUrl, string onetimeToken, TimeSpan timeout)
        {
            return new SamlRestRequest()
            {
                Url = ssoUrl,
                RestTimeout = timeout,
                HttpTimeout = Timeout.InfiniteTimeSpan,
                OnetimeToken = onetimeToken,
            };
        }
    }
}