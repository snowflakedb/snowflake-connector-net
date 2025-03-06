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

        public static SFSessionProperties GetKeyPairFromFileContentParameters(string privateKey)
        {

            var properties = GetBaseConnectionParameters();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "snowflake_jwt");
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.PRIVATE_KEY, privateKey);

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
                           SslProtocols = SslProtocols.Tls12,
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
