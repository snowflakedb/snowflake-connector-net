using System;
using System.Net.Http;
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
using Snowflake.Data.Core.Extensions;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// KeyPairAuthenticator is used for Key pair based authentication.
    /// See <see cref="https://docs.snowflake.com/en/user-guide/key-pair-auth.html"/> for more information.
    /// </summary>
    internal sealed class KeyPairAuthenticator : BaseAuthenticator, IAuthenticator
    {
        private const string AUTH_NAME = "snowflake_jwt";
        private const string KEY_PAIR_AUTHENTICATOR_SETUP_KEY = "KEY_PAIR_AUTHENTICATOR_SETUP_KEY";
        private const string ClientType = "auth";

        private static readonly SFLogger s_logger =
            SFLoggerFactory.GetLogger<KeyPairAuthenticator>();

        private readonly RSACryptoServiceProvider _rsaProvider;

        internal KeyPairAuthenticator(SFSession session) : this(session, GetRestRequester(session))
        {
        }

        internal KeyPairAuthenticator(SFSession session, IRestRequester customRequester) : base(session, customRequester)
        {
            this.Session = session;
            _rsaProvider = new RSACryptoServiceProvider();
        }

        private static IRestRequester GetRestRequester(SFSession session)
        {
            var authHandler = new AuthenticationHandler();
            var config = session.GetHttpClientConfig(ClientType);
            var httpClient = HttpUtil.Instance.GetHttpClient(config, authHandler);
            return new RestRequester(httpClient);
        }

        public static bool IsKeyPairAuthenticator(string authenticator) =>
            AUTH_NAME.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        public Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            s_logger.Debug("Send login request");
            return LoginAsync(cancellationToken);
        }

        public void Authenticate()
        {
            s_logger.Debug("Send login request");
            base.Login();
        }

        protected override string AuthName => AUTH_NAME;

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.Token = GenerateJwtToken();
            SetSecondaryAuthenticationData(ref data);
        }

        protected override SFRestRequest BuildLoginRequest()
        {
            var baseRequest = base.BuildLoginRequest();
            var rotator = new JwtRotator(baseRequest, this);
            baseRequest.Options[KEY_PAIR_AUTHENTICATOR_SETUP_KEY] = rotator;
            return baseRequest;
        }

        private string GenerateJwtToken()
        {
            s_logger.Info("Key-pair Authentication");

            var hasPkPath = Session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_FILE, out var pkPath);
            _ = Session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY, out var pkContent);
            Session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_PWD, out var pkPwd);

            // Extract the public key from the private key to generate the fingerprints
            RSAParameters rsaParams;
            string publicKeyFingerPrint = null;
            AsymmetricCipherKeyPair keypair = null;
            using (TextReader tr =
                   hasPkPath ? new StreamReader(pkPath) : new StringReader(pkContent))
            {
                try
                {
                    using var pr = CreatePemReader(tr, pkPwd);
                    var key = pr.ReadObject();
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
                        var pk = (RsaPrivateCrtKeyParameters)key;
                        rsaParams = DotNetUtilities.ToRSAParameters(pk);
                        keypair = DotNetUtilities.GetRsaKeyPair(rsaParams);
                    }

                    if (keypair == null)
                    {
                        throw new Exception("Unknown error.");
                    }
                }
                catch (Exception e)
                {
                    throw new SnowflakeDbException(
                        e,
                        SFError.JWT_ERROR_READING_PK,
                        args:
                        [
                            hasPkPath ? pkPath : "with value passed in connection string",
                            (pkContent == null)
                                ? e.ToString()
                                : "incorrect private key value or " +
                                  "private key format: use \"\\n\" for newlines and double the equals sign."
                        ]);
                }
            }

            // Generate the public key fingerprint
            var publicKey = keypair.Public;
            var publicKeyEncoded =
                SubjectPublicKeyInfoFactory.CreateSubjectPublicKeyInfo(publicKey).GetDerEncoded();
            using (var SHA256Encoder = SHA256.Create())
            {
                var sha256Hash = SHA256Encoder.ComputeHash(publicKeyEncoded);
                publicKeyFingerPrint = "SHA256:" + Convert.ToBase64String(sha256Hash);
            }

            // Generating the token
            var now = DateTime.UtcNow;
            var dtDateTime =
                new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            var secondsSinceEpoch = (long)((now - dtDateTime).TotalSeconds);

            /*
             * Payload content
             *      iss : $accountName.$userName.$publicKeyFingerprint
             *      sub : $accountName.$userName
             *      iat : $now
             *      exp : $now + LIFETIME
             *
             * Note : Lifetime = 120sec for Python impl, 60sec for Jdbc and Odbc
             */
            var accountUser =
                Session.properties[SFSessionProperty.ACCOUNT].ToUpper() +
                "." +
                Session.properties[SFSessionProperty.USER].ToUpper();
            var issuer = accountUser + "." + publicKeyFingerPrint;
            var claims = new[]
            {
                new Claim(
                    JwtRegisteredClaimNames.Iat,
                    secondsSinceEpoch.ToString(),
                    ClaimValueTypes.Integer64),
                new Claim(JwtRegisteredClaimNames.Sub, accountUser),
            };

            _rsaProvider.ImportParameters(rsaParams);
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
                    new RsaSecurityKey(_rsaProvider), SecurityAlgorithms.RsaSha256)
            );

            // Serialize the jwt token
            // Base64URL-encoded parts delimited by period ('.'), with format :
            //     [header-base64url].[payload-base64url].[signature-base64url]
            var handler = new JwtSecurityTokenHandler();
            var jwtToken = handler.WriteToken(token);

            return jwtToken;
        }

        private static PemReader CreatePemReader(TextReader textReader, string privateKeyPassword)
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
            private string password;

            public PasswordFinder(string password)
            {
                this.password = password;
            }

            public char[] GetPassword()
            {
                if (null == password || (0 == password.Length))
                    return null;

                return password.ToCharArray();
            }
        }

        private sealed class JwtRotator
        {
            private readonly SFRestRequest _request;
            private readonly KeyPairAuthenticator _authenticator;
            private bool _hasRunBefore;

            public JwtRotator(SFRestRequest request, KeyPairAuthenticator authenticator)
            {
                _request = request;
                _authenticator = authenticator;
            }

            public void Rotate(HttpRequestMessage r)
            {
                if (!_hasRunBefore)
                {
                    _hasRunBefore = true;
                    return;
                }

                r.Content?.Dispose();
                ((LoginRequest)_request.jsonBody).data.Token = _authenticator.GenerateJwtToken();
                r.Content = ((IRestRequest)_request).ToRequestMessage(HttpMethod.Post).Content;
            }
        }

        private sealed class AuthenticationHandler : DelegatingHandler
        {
            protected override Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                if (!request.TryGetOption(KEY_PAIR_AUTHENTICATOR_SETUP_KEY, out var setupObj)
                    || setupObj is not JwtRotator jwtRotator)
                    return base.SendAsync(request, cancellationToken);

                jwtRotator.Rotate(request);
                return base.SendAsync(request, cancellationToken);
            }
        }
    }
}


