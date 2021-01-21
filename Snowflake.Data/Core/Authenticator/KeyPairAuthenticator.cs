/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

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
        public static readonly string AUTH_NAME = "snowflake_jwt";

        // The logger.
        private static readonly SFLogger logger =
            SFLoggerFactory.GetLogger<KeyPairAuthenticator>();

        // The jwt token to send in the login request.
        private string jwtToken;

        /// <summary>
        /// Constructor for the Key-Pair authenticator.
        /// </summary>
        /// <param name="session">Session which created this authenticator</param>
        internal KeyPairAuthenticator(SFSession session) : base(session, AUTH_NAME)
        {
            this.session = session;
        }

        /// <see cref="IAuthenticator.AuthenticateAsync"/>
        async public Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            GenerateJwtToken();

            // Send the http request with the generate token
            logger.Debug("Send login request");
            await base.LoginAsync(cancellationToken);
        }

        /// <see cref="IAuthenticator.Authenticate"/>
        public void Authenticate()
        {
            GenerateJwtToken();

            // Send the http request with the generate token
            logger.Debug("Send login request");
            base.Login();
        }

        /// <see cref="BaseAuthenticator.SetSpecializedAuthenticatorData(ref LoginRequestData)"/>
        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            // Add the token to the Data attribute
            data.Token = jwtToken;
        }

        /// <summary>
        /// Generates a JwtToken to use for login.
        /// </summary>
        /// <param name="pkPath">The path to the private key file.</param>
        /// <param name="pkPwd">The password to decrypt the key or null if no password</param>
        /// <param name="account">The Snowflake account.</param>
        /// <param name="user">The Snowflake user.</param>
        /// <returns>The generated JWT token.</returns>
        private string GenerateJwtToken()
        {
            logger.Info("Key-pair Authentication");

            var pkPath = session.properties[SFSessionProperty.PRIVATE_KEY_FILE];
            session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_FILE_PWD, out var pkPwd);

            // Extract the public key from the private key to generate the fingerprints
            RSAParameters rsaParams;
            String publicKeyFingerPrint = null;
            AsymmetricCipherKeyPair keypair = null;
            using (TextReader tr = new StreamReader(pkPath))
            {
                try
                {
                    if (null != pkPwd)
                    {
                        // Encrypted key
                        IPasswordFinder ipwdf = new PasswordFinder(pkPwd);
                        PemReader pr = new PemReader(tr, ipwdf);
                        RsaPrivateCrtKeyParameters pk = pr.ReadObject() as RsaPrivateCrtKeyParameters;
                        rsaParams = DotNetUtilities.ToRSAParameters(pk);
                        keypair = DotNetUtilities.GetRsaKeyPair(rsaParams);
                    }
                    else
                    {
                        // Unencrypted key
                        keypair = new PemReader(tr).ReadObject() as AsymmetricCipherKeyPair;
                        rsaParams = DotNetUtilities.ToRSAParameters(
                            keypair.Private as RsaPrivateCrtKeyParameters);
                    }
                    if (keypair == null)
                    {
                        throw new Exception("Unknown error.");
                    }
                }
                catch (Exception e)
                {
                    throw new SnowflakeDbException(
                        SFError.JWT_ERROR_READING_PK,
                        pkPath,
                        e.ToString(),
                        e);
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
                logger.Debug("publicKeyFingerPrint " + publicKeyFingerPrint);
            }
            
            // Generating the token 
            var now = DateTime.UtcNow;
            System.DateTime dtDateTime =
                new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            long secondsSinceEpoch = (long)((now - dtDateTime).TotalSeconds);

            /* 
             * Payload content
             *      iss : $accountName.$userName.$pulicKeyFingerprint
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

            using (RSACryptoServiceProvider rsaProvider = new RSACryptoServiceProvider())
            {
                logger.Debug("claims " + claims);
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

                logger.Debug("Unserialized token " + token);

                logger.Debug("encoded header " + token.EncodedHeader);
                logger.Debug("encoded payload " + token.EncodedPayload);
                logger.Debug("SignatureAlgorithm " + token.SignatureAlgorithm);

                // Serialize the jwt token
                // Base64URL-encoded parts delimited by period ('.'), with format :
                //     [header-base64url].[payload-base64url].[signature-base64url]
                var handler = new JwtSecurityTokenHandler();
                return handler.WriteToken(token);
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


