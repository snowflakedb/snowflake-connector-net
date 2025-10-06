using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using System.IdentityModel.Tokens.Jwt;
using Snowflake.Data.Client;
using System.Security.Cryptography;
using System.IO;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.X509;
using System.Security.Claims;
using Microsoft.IdentityModel.Tokens;
namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// KeyPairAuthenticator is used for Key pair based authentication.
    /// See <see cref="https://docs.snowflake.com/en/user-guide/key-pair-auth.html"/> for more information.
    /// </summary>
    class KeyPairAuthenticator : BaseAuthenticator, IAuthenticator
    {
        // The authenticator setting value to use to authenticate using key pair authentication.
        public const string AUTH_NAME = "snowflake_jwt";

        // The logger.
        private static readonly SFLogger logger =
            SFLoggerFactory.GetLogger<KeyPairAuthenticator>();

        // The RSA provider to use to sign the tokens
        private RSACryptoServiceProvider rsaProvider;

        // The jwt token to send in the login request.
        private string jwtToken;

        /// <summary>
        /// Constructor for the Key-Pair authenticator.
        /// </summary>
        /// <param name="session">Session which created this authenticator</param>
        internal KeyPairAuthenticator(SFSession session) : base(session, AUTH_NAME)
        {
            this.session = session;
            this.rsaProvider = new RSACryptoServiceProvider();
        }

        public static bool IsKeyPairAuthenticator(string authenticator) =>
            AUTH_NAME.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        /// <see cref="IAuthenticator.AuthenticateAsync"/>
        async public Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            jwtToken = GenerateJwtToken();

            // Send the http request with the generate token
            logger.Debug("Send login request");
            await base.LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <see cref="IAuthenticator.Authenticate"/>
        public void Authenticate()
        {
            jwtToken = GenerateJwtToken();

            // Send the http request with the generate token
            logger.Debug("Send login request");
            base.Login();
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            // Add the token to the Data attribute
            data.Token = jwtToken;
            SetSecondaryAuthenticationData(ref data);
        }

        /// <summary>
        /// Generates a JwtToken to use for login.
        /// </summary>
        /// <returns>The generated JWT token.</returns>
        private string GenerateJwtToken()
        {
            logger.Info("Key-pair Authentication");

            bool hasPkPath =
                session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_FILE, out var pkPath);
            bool hasPkContent =
                session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY, out var pkContent);
            session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_PWD, out var pkPwd);

            // Extract the public key from the private key to generate the fingerprints
            RSAParameters rsaParams;
            String publicKeyFingerPrint = null;
            AsymmetricCipherKeyPair keypair = null;
            using (TextReader tr =
                hasPkPath ? (TextReader)new StreamReader(pkPath) : new StringReader(pkContent))
            {
                try
                {
                    using (PemReader pr = CreatePemReader(tr, pkPwd))
                    {
                        object key = pr.ReadObject();
                        // Infer what the pem reader is sending back based on the object properties
                        if (key.GetType().GetProperty("Private") != null)
                        {
                            // PKCS1 key
                            keypair = (AsymmetricCipherKeyPair)key;
                            rsaParams = DotNetUtilities.ToRSAParameters(
                                keypair.Private as RsaPrivateCrtKeyParameters);
                        }
                        else
                        {
                            // PKCS8 key
                            RsaPrivateCrtKeyParameters pk = (RsaPrivateCrtKeyParameters)key;
                            rsaParams = DotNetUtilities.ToRSAParameters(pk);
                            keypair = DotNetUtilities.GetRsaKeyPair(rsaParams);
                        }

                        if (keypair == null)
                        {
                            throw new Exception("Unknown error.");
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new SnowflakeDbException(
                        e,
                        SFError.JWT_ERROR_READING_PK,
                        hasPkPath ? pkPath : "with value passed in connection string",
                        (pkContent == null) ? e.ToString() : "incorrect private key value or " +
                        "private key format: use \"\\n\" for newlines and double the equals sign.");
                }
            }

            // Generate the public key fingerprint
            var publicKey = keypair.Public;
            byte[] publicKeyEncoded =
                SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(publicKey).GetDerEncoded();
            using (SHA256 SHA256Encoder = SHA256.Create())
            {
                byte[] sha256Hash = SHA256Encoder.ComputeHash(publicKeyEncoded);
                publicKeyFingerPrint = "SHA256:" + Convert.ToBase64String(sha256Hash);
            }

            // Generating the token
            var now = DateTime.UtcNow;
            System.DateTime dtDateTime =
                new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            long secondsSinceEpoch = (long)((now - dtDateTime).TotalSeconds);

            /*
             * Payload content
             *      iss : $accountName.$userName.$publicKeyFingerprint
             *      sub : $accountName.$userName
             *      iat : $now
             *      exp : $now + LIFETIME
             *
             * Note : Lifetime = 120sec for Python impl, 60sec for Jdbc and Odbc
            */
            String accountUser =
                session.properties[SFSessionProperty.ACCOUNT].ToUpper() +
                "." +
                session.properties[SFSessionProperty.USER].ToUpper();
            String issuer = accountUser + "." + publicKeyFingerPrint;
            var claims = new[] {
                        new Claim(
                            JwtRegisteredClaimNames.Iat,
                            secondsSinceEpoch.ToString(),
                            System.Security.Claims.ClaimValueTypes.Integer64),
                        new Claim(JwtRegisteredClaimNames.Sub, accountUser),
                    };

            rsaProvider.ImportParameters(rsaParams);
            var token = new JwtSecurityToken(
                // Issuer
                issuer,
                // Audience
                null,
                // Subject
                claims,
                //NotBefore
                null,
                // Expires
                now.AddSeconds(60),
                //SigningCredentials
                new SigningCredentials(
                    new RsaSecurityKey(rsaProvider), SecurityAlgorithms.RsaSha256)
            );

            // Serialize the jwt token
            // Base64URL-encoded parts delimited by period ('.'), with format :
            //     [header-base64url].[payload-base64url].[signature-base64url]
            var handler = new JwtSecurityTokenHandler();
            string jwtToken = handler.WriteToken(token);

            return jwtToken;
        }

        private PemReader CreatePemReader(TextReader textReader, string privateKeyPassword)
        {
            if (null != privateKeyPassword)
            {
                IPasswordFinder ipwdf = new PasswordFinder(privateKeyPassword);
                return new PemReader(textReader, ipwdf);
            }
            else
            {
                return new PemReader(textReader);
            }
        }

        /// <summary>
        /// Helper class to handle the password for the certificate if there is one.
        /// </summary>
        private class PasswordFinder : IPasswordFinder
        {
            // The password.
            private string password;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="password">The password.</param>
            public PasswordFinder(string password)
            {
                this.password = password;
            }

            /// <summary>
            /// Returns the password or null if the password is empty or null.
            /// </summary>
            /// <returns>The password or null if the password is empty or null.</returns>
            public char[] GetPassword()
            {
                if ((null == password) || (0 == password.Length))
                {
                    // No password.
                    return null;
                }
                else
                {
                    return password.ToCharArray();
                }
            }
        }
    }
}


