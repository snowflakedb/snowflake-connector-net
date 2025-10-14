using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System.IO;
using System.Net;
using Snowflake.Data.Core;
using System.Net.Http;
using System.Security.Authentication;

namespace Snowflake.Data.AuthenticationTests

{
    static class AuthConnectionString
    {
        public static readonly string SsoUser = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_BROWSER_USER");
        public static readonly string Host = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_HOST");
        public static readonly string SsoPassword = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_OKTA_PASS");
        public static readonly string SnowflakeUser = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_SNOWFLAKE_USER");
        public static readonly string SnowflakeRole = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE");


        private static SFSessionProperties GetBaseConnectionParameters()
        {
            var properties = new SFSessionProperties()
            {
                {SFSessionProperty.HOST, Host },
                {SFSessionProperty.PORT, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_PORT") },
                {SFSessionProperty.ROLE, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_ROLE") },
                {SFSessionProperty.ACCOUNT, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_ACCOUNT") },
                {SFSessionProperty.DB, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_DATABASE") },
                {SFSessionProperty.SCHEMA, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_SCHEMA") },
                {SFSessionProperty.WAREHOUSE, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_WAREHOUSE") },
                {SFSessionProperty.MINPOOLSIZE, "0"},
                {SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, "false"},
                {SFSessionProperty.CERTREVOCATIONCHECKMODE, "enabled"}
            };
            return properties;
        }

        public static SFSessionProperties GetSnowflakeLoginCredentials()
        {
            var properties = new SFSessionProperties()
            {
                { SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID") },
                { SFSessionProperty.PASSWORD, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_USER_PASSWORD") }
            };

            return properties;
        }

        public static SFSessionProperties GetExternalBrowserConnectionString()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "externalbrowser");
            properties.Add(SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_BROWSER_USER"));
            return properties;
        }

        public static SFSessionProperties GetOAuthExternalAuthorizationCodeConnectionString()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "OAUTH_AUTHORIZATION_CODE");
            properties.Add(SFSessionProperty.OAUTHCLIENTID, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
            properties.Add(SFSessionProperty.OAUTHCLIENTSECRET, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET"));
            properties.Add(SFSessionProperty.OAUTHREDIRECTURI, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_REDIRECT_URI"));
            properties.Add(SFSessionProperty.OAUTHAUTHORIZATIONURL, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_AUTH_URL"));
            properties.Add(SFSessionProperty.OAUTHTOKENREQUESTURL, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN"));
            properties.Add(SFSessionProperty.USER, SsoUser);

            return properties;
        }

        public static SFSessionProperties GetOAuthSnowflakeAuthorizationCodeConnectionParameters()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "OAUTH_AUTHORIZATION_CODE");
            properties.Add(SFSessionProperty.OAUTHCLIENTID, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_ID"));
            properties.Add(SFSessionProperty.OAUTHCLIENTSECRET, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_SECRET"));
            properties.Add(SFSessionProperty.OAUTHREDIRECTURI, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_REDIRECT_URI"));
            properties[SFSessionProperty.ROLE] = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE");
            properties.Add(SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));

            return properties;
        }

        public static SFSessionProperties GetOAuthSnowflakeAuthorizationCodeWilidcardsConnectionParameters()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "OAUTH_AUTHORIZATION_CODE");
            properties.Add(SFSessionProperty.OAUTHCLIENTID, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_WILDCARDS_CLIENT_ID"));
            properties.Add(SFSessionProperty.OAUTHCLIENTSECRET, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_WILDCARDS_CLIENT_SECRET"));
            properties[SFSessionProperty.ROLE] = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE");
            properties.Add(SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));

            return properties;
        }

        public static SFSessionProperties GetOAuthExternalClientCredentialParameters()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "OAUTH_CLIENT_CREDENTIALS");
            properties.Add(SFSessionProperty.OAUTHCLIENTID, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
            properties.Add(SFSessionProperty.OAUTHCLIENTSECRET, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET"));
            properties.Add(SFSessionProperty.OAUTHTOKENREQUESTURL, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN"));
            properties.Add(SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));

            return properties;
        }

        public static SFSessionProperties GetOauthConnectionString(string token)
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "OAUTH");
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.TOKEN, token);
            return properties;
        }

        public static SFSessionProperties GetOktaConnectionString()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_URL"));
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.PASSWORD, SsoPassword);

            return properties;
        }

        public static SFSessionProperties GetMfaConnectionString()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "USERNAME_PASSWORD_MFA");
            properties.Add(SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_MFA_USER"));
            properties.Add(SFSessionProperty.PASSWORD, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_MFA_PASSWORD"));

            return properties;
        }

        public static SFSessionProperties GetKeyPairFromFileContentParameters(string privateKey)
        {

            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "snowflake_jwt");
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.PRIVATE_KEY, privateKey);

            return properties;
        }

        public static SFSessionProperties GetPatConnectionParameters()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "PROGRAMMATIC_ACCESS_TOKEN");
            properties.Add(SFSessionProperty.USER, SsoUser);
            return properties;
        }

        public static SFSessionProperties GetKeyPairFromFilePathConnectionString(string privateKeyPath)
        {

            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "snowflake_jwt");
            properties.Add(SFSessionProperty.USER, AuthConnectionString.SsoUser);
            properties.Add(SFSessionProperty.PRIVATE_KEY_FILE, privateKeyPath);
            return properties;
        }

        public static string ConvertToConnectionString(SFSessionProperties properties)
        {
            StringBuilder connectionStringBuilder = new StringBuilder();

            foreach (var property in properties)
            {
                connectionStringBuilder.Append($"{property.Key.ToString().ToLower()}={property.Value};");
            }
            return connectionStringBuilder.ToString();
        }

        public static string GetPrivateKeyContentForKeypairAuth(string fileLocation)
        {
            string filePath = Environment.GetEnvironmentVariable(fileLocation);
            Assert.IsNotNull(filePath);
            string pemKey = File.ReadAllText(Path.Combine("..", "..", "..", "..", filePath));
            Assert.IsNotNull(pemKey, $"Failed to read file: {filePath}");
            return pemKey;

        }

        public static string GetPrivateKeyPathForKeypairAuth(string relativeFileLocationEnvVariable)
        {
            string filePath = Environment.GetEnvironmentVariable(relativeFileLocationEnvVariable);
            Assert.IsNotNull(filePath);
            return Path.Combine("..", "..", "..", "..", filePath);
        }

        public static string GetOauthToken()
        {
            try
            {
                using (var client = new HttpClient(new HttpClientHandler
                {
                    CheckCertificateRevocationList = true,
                    SslProtocols = SslProtocols.Tls12 | SslProtocolsExtensions.Tls13,
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                    UseProxy = false,
                    UseCookies = false
                }))
                {
                    var authUrl = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_URL");
                    var clientId = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID");
                    var clientSecret = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET");
                    var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));

                    client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", credentials);

                    var values = new Dictionary<string, string>
                    {
                        { "username", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OKTA_USER") },
                        { "password", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OKTA_PASS") },
                        { "grant_type", "password" },
                        { "scope", "session:role:" + Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_ROLE") }
                    };

                    var content = new FormUrlEncodedContent(values);
                    var response = client.PostAsync(authUrl, content).Result;
                    response.EnsureSuccessStatusCode();

                    var fullResponse = response.Content.ReadAsStringAsync().Result;
                    var responseObject = JObject.Parse(fullResponse);
                    Assert.IsNotNull(responseObject["access_token"]);
                    return responseObject["access_token"].ToString();
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to get OAuth token: {e.Message}");
            }
        }
    }
}
