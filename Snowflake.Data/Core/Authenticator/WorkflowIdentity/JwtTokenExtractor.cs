using System;
using System.IdentityModel.Tokens.Jwt;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class JwtTokenExtractor
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<JwtTokenExtractor>();

        public JwtSecurityToken ReadJwtToken(string token, Func<string, SnowflakeDbException> exceptionBuilder)
        {
            var handler = new JwtSecurityTokenHandler();
            try
            {
                return handler.ReadJwtToken(token);
            }
            catch (Exception)
            {
                s_logger.Error("Reading of the token failed.");
                throw exceptionBuilder("Reading of the token failed.");
            }
        }
    }
}
