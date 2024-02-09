using System;
using Snowflake.Data.Core.Authenticator.Okta.Models;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal class IdpTokenRestRequestFactory : IIdpTokenRestRequestFactory
    {
        private readonly TimeSpan _httpTimeout = TimeSpan.FromSeconds(16);
        
        public IdpTokenRestRequest Create(Uri tokenUrl, SFSession session)
        {
            return new IdpTokenRestRequest()
            {
                Url = tokenUrl,
                RestTimeout = session.connectionTimeout,
                HttpTimeout = _httpTimeout,
                JsonBody = new IdpTokenRequest()
                {
                    Username = session.properties[SFSessionProperty.USER],
                    Password = session.properties[SFSessionProperty.PASSWORD],
                },
            };
        }
    }
}