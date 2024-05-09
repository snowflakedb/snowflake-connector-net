using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    internal static class AuthenticationPropertiesValidator
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFSessionProperties>();

        public static void Validate(SFSessionProperties properties)
        {
            if (!properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator))
            {
                s_logger.Error("Unspecified authenticator");
                throw new SnowflakeDbException(SFError.UNKNOWN_AUTHENTICATOR);
            }

            bool valid;
            var error = "";
            switch (authenticator.ToLower())
            {
                case BasicAuthenticator.AUTH_NAME:
                    valid = !string.IsNullOrEmpty(properties[SFSessionProperty.USER]) &&
                            !string.IsNullOrEmpty(properties[SFSessionProperty.PASSWORD]);
                    if (!valid)
                        error = $"USER and PASSWORD should be provided for Authenticator={authenticator}";
                    break;
                case ExternalBrowserAuthenticator.AUTH_NAME:
                    valid = true;
                    break;
                case KeyPairAuthenticator.AUTH_NAME:
                    valid = (properties.TryGetValue(SFSessionProperty.PRIVATE_KEY, out var privateKey)
                             && !string.IsNullOrEmpty(privateKey)) ||
                            (properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_FILE, out var privateKeyFile) &&
                             !string.IsNullOrEmpty(privateKeyFile));
                    if (!valid)
                        error = $"PRIVATE_KEY or PRIVATE_KEY_FILE should be provided for Authenticator={authenticator}";
                    break;
                case OAuthAuthenticator.AUTH_NAME:
                    valid = !string.IsNullOrEmpty(properties[SFSessionProperty.TOKEN]);
                    if (!valid)
                        error = $"TOKEN should be provided for Authenticator={authenticator}";
                    break;
                default:
                    if (authenticator.Contains(OktaAuthenticator.AUTH_NAME) &&
                        authenticator.StartsWith("https://"))
                    {
                        valid = !string.IsNullOrEmpty(properties[SFSessionProperty.USER]) &&
                                !string.IsNullOrEmpty(properties[SFSessionProperty.PASSWORD]);
                        if (!valid)
                            error = $"USER and PASSWORD should be provided for Authenticator={authenticator}";
                    }
                    else
                        throw new SnowflakeDbException(SFError.UNKNOWN_AUTHENTICATOR, $"Unrecognized Authenticator={authenticator}");
                    break;
            }

            if (!valid)
            {
                s_logger.Error(error);
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, $"Unrecognized Authenticator={authenticator}");
            }
        }
    }
}
