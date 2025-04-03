using System;
using System.Security;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.CredentialManager
{
    internal class Token
    {
        public SecureString Value { get; set; }
        public DateTime? UtcExpirationTime { get; set; }

        public string ExtractToken(DateTime utcNow)
        {
            if (UtcExpirationTime != null && UtcExpirationTime.Value <= utcNow)
                return null;
            return SecureStringHelper.Decode(Value);
        }
    }
}
