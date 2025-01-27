using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System.IO;
using Snowflake.Data.Core;
using System.Net.Http;

namespace Snowflake.Data.Tests.AuthenticationTests

{
    static class AuthConnectionString
    {
        public static readonly string SsoUser = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_BROWSER_USER");
        public static readonly string Host = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_HOST");
        public static readonly string SsoPassword = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_OKTA_PASS");

        public static SFSessionProperties GetBaseConnectionString()
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

        public static string SetBaseConnectionString(SFSessionProperties parameters) =>
            $"host={parameters[SFSessionProperty.HOST]};port={parameters[SFSessionProperty.PORT]};account={parameters[SFSessionProperty.ACCOUNT]};role={parameters[SFSessionProperty.ROLE]};db={parameters[SFSessionProperty.DB]};schema={parameters[SFSessionProperty.SCHEMA]};warehouse={parameters[SFSessionProperty.WAREHOUSE]};";


        public static SFSessionProperties GetExternalBrowserConnectionString()
        {
            var properties = GetBaseConnectionString();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "externalbrowser");
            properties.Add(SFSessionProperty.USER, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_BROWSER_USER"));
            return properties;
        }

        public static string SetExternalBrowserConnectionString(SFSessionProperties parameters) =>
            $"{SetBaseConnectionString(parameters)}authenticator={parameters[SFSessionProperty.AUTHENTICATOR]};user={parameters[SFSessionProperty.USER]};";


        public static SFSessionProperties GetOauthConnectionString(string token)
        {
            var properties = GetBaseConnectionString();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "OAUTH");
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.TOKEN, token);
            return properties;
        }

        public static string SetOauthConnectionString(SFSessionProperties parameters) =>
            $"{SetBaseConnectionString(parameters)}authenticator={parameters[SFSessionProperty.AUTHENTICATOR]};user={parameters[SFSessionProperty.USER]};token={parameters[SFSessionProperty.TOKEN]};";

        public static SFSessionProperties GetOktaConnectionString()
        {
            var properties = GetBaseConnectionString();
            properties.Add(SFSessionProperty.AUTHENTICATOR, Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_URL"));
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.PASSWORD, SsoPassword);

            return properties;
        }

        public static SFSessionProperties GetKeyPairFromFileContentParameters(string privateKey)
        {

            var properties = GetBaseConnectionString();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "snowflake_jwt");
            properties.Add(SFSessionProperty.USER, SsoUser);
            properties.Add(SFSessionProperty.PRIVATE_KEY, privateKey);

            return properties;
        }


        public static SFSessionProperties GetKeyPairFromFilePathConnectionString(string privateKeyPath)
        {

            var properties = GetBaseConnectionString();
            properties.Add(SFSessionProperty.AUTHENTICATOR, "snowflake_jwt");
            properties.Add(SFSessionProperty.USER, AuthConnectionString.SsoUser);
            properties.Add(SFSessionProperty.PRIVATE_KEY_FILE, privateKeyPath);
            return properties;
        }
        public static string SetPrivateKeyFromFileContentConnectionString(SFSessionProperties parameters) =>
            $"{SetBaseConnectionString(parameters)}authenticator={parameters[SFSessionProperty.AUTHENTICATOR]};private_key={parameters[SFSessionProperty.PRIVATE_KEY]};user={SsoUser}";


        public static string SetPrivateKeyFromFilePathConnectionString(SFSessionProperties parameters) =>
            $"{SetBaseConnectionString(parameters)}authenticator={parameters[SFSessionProperty.AUTHENTICATOR]};private_key_file={parameters[SFSessionProperty.PRIVATE_KEY_FILE]};user={SsoUser}";


        public static string SetOktaConnectionString(SFSessionProperties parameters) =>
            $"{SetBaseConnectionString(parameters)}authenticator={parameters[SFSessionProperty.AUTHENTICATOR]};user={parameters[SFSessionProperty.USER]};password={parameters[SFSessionProperty.PASSWORD]};";


        public static string GetPrivateKeyContentForKeypairAuth(string fileLocation)
        {
            string filePath = Environment.GetEnvironmentVariable(fileLocation);
            Assert.IsNotNull(filePath);
            string pemKey = File.ReadAllText(Path.Combine("..", "..", "..", "..", filePath));
            Assert.IsNotNull(pemKey, $"Failed to read file: {filePath}");
            return pemKey;

        }

        public static string GetPrivateKeyPathForKeypairAuth(string fileLocation)
        {
            string filePath = Environment.GetEnvironmentVariable(fileLocation);
            Assert.IsNotNull(filePath);
            return Path.Combine("..", "..", "..", "..", filePath);
        }

        public static string GetOauthToken()
        {
            try
            {

                using (var client = new HttpClient())
                {
                    var authUrl = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_URL");
                    var clientId = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID");
                    var clientSecret = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET");
                    var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{clientId}:{clientSecret}"));

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
                Assert.Fail($"Failed to get OAuth token: {e.Message}");
                return null;
            }

        }
    }
}
