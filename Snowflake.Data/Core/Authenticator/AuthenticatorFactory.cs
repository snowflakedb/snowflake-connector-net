using System;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.Okta;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// Authenticator Factory to build authenticators
    /// </summary>
    internal class AuthenticatorFactory
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<AuthenticatorFactory>();
        /// <summary>
        /// Generate the authenticator given the session
        /// </summary>
        /// <param name="session">session that requires the authentication</param>
        /// <returns>authenticator</returns>
        /// <exception cref="SnowflakeDbException">when authenticator is unknown</exception>
        internal static IAuthenticator GetAuthenticator(SFSession session)
        {
            string type = session.properties[SFSessionProperty.AUTHENTICATOR];
            if (type.Equals(BasicAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                return new BasicAuthenticator(session);
            }

            if (type.Equals(ExternalBrowserAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                return new ExternalBrowserAuthenticator(session);
            }

            if (type.Equals(KeyPairAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                // Get private key path or private key from connection settings
                if (!session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_FILE, out var pkPath) &&
                    !session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY, out var pkContent))
                {
                    // There is no PRIVATE_KEY_FILE defined, can't authenticate with key-pair
                    string invalidStringDetail =
                        "Missing required PRIVATE_KEY_FILE or PRIVATE_KEY for key pair authentication";
                    var error = new SnowflakeDbException(
                        SFError.INVALID_CONNECTION_STRING,
                        new object[] { invalidStringDetail });
                    logger.Error(error.Message, error);
                    throw error;
                }

                return new KeyPairAuthenticator(session);
            }
            if (type.Equals(OAuthAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                // Get private key path or private key from connection settings
                if (!session.properties.TryGetValue(SFSessionProperty.TOKEN, out var pkPath))
                {
                    // There is no TOKEN defined, can't authenticate with oauth
                    string invalidStringDetail =
                        "Missing required TOKEN for Oauth authentication";
                    var error = new SnowflakeDbException(
                        SFError.INVALID_CONNECTION_STRING,
                        new object[] { invalidStringDetail });
                    logger.Error(error.Message, error);
                    throw error;
                }

                return new OAuthAuthenticator(session);
            }
            // Okta would provide a url of form: https://xxxxxx.okta.com or https://xxxxxx.oktapreview.com or https://vanity.url/snowflake/okta
            if (type.Contains("okta") && type.StartsWith("https://"))
            {
                return new OktaAuthenticator(session, type, new SamlRestRequestFactory(), new IdpTokenRestRequestFactory());
            }

            var e = new SnowflakeDbException(SFError.UNKNOWN_AUTHENTICATOR, type);

            logger.Error("Unknown authenticator", e);

            throw e;
        }
    }
}