using System;
using Snowflake.Data.Core.Authenticator.Okta.Models;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Core.Authenticator.Okta
{
    internal interface IIdpTokenRestRequestFactory
    {
        IdpTokenRestRequest Create(Uri tokenUrl, SFSession  session);
    }
}