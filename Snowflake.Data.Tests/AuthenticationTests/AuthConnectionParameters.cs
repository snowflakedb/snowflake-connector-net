using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Collections.Specialized;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System.IO;

namespace Snowflake.Data.Tests.AuthenticationTests

{
    static class AuthConnectionParameters
    {
        public static readonly string SsoUser = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_BROWSER_USER");
        public static readonly string Host = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_HOST");
        public static readonly string SsoPassword = Environment.GetEnvironmentVariable("SNOWFLAKE_TEST_OKTA_PASS");
        public static readonly string Okta = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OKTA_NAME");
        public static readonly string OauthPassword = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_USER_PASSWORD");
        public static Dictionary<string, string> GetBaseConnectionParameters()
        {
            var properties = new Dictionary<string, string>
            {
                { "host", Host },
                { "protocol", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_PROTOCOL")},
                { "port", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_PORT") },
                { "role", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_ROLE") },
                { "account", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_ACCOUNT") },
                { "db", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_DATABASE") },
                { "schema", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_SCHEMA") },
                { "warehouse", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_WAREHOUSE") },
            };
            return properties;
        }

        public static String SetBaseConnectionParameters(Dictionary<string, string> param)
        {
            var basicConfig = String.Format("host={0};port={1};account={2};role={3};db={4};schema={5};warehouse={6};",
                param["host"], param["port"], param["account"], param["role"], param["db"], param["schema"], param["warehouse"]);
            return basicConfig;
        }

        public static Dictionary<string, string> GetExternalBrowserConnectionParameters()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add("authenticator", "externalbrowser");
            properties.Add("user", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_BROWSER_USER"));
            // properties.Add("user", SsoUser);
            return properties;
        }

        public static String SetExternalBrowserConnectionParameters(Dictionary<string, string> param)
        {
            var config = String.Format("{0}authenticator={1};user={2};", SetBaseConnectionParameters(param), param["authenticator"], param["user"]);
            return config;
        }

        public static Dictionary<string, string> GetOauthConnectionParameters(string token)
        {
            var properties = GetBaseConnectionParameters();
            properties.Add("authenticator", "OAUTH");
            properties.Add("user", SsoUser);
            properties.Add("token", token);
            return properties;
        }

        public static String SetOauthConnectionParameters(Dictionary<string, string> param)
        {
            var config = String.Format("{0}authenticator={1};user={2};token={3}", SetBaseConnectionParameters(param), param["authenticator"], param["user"], param["token"]);
            return config;
        }

        public static Dictionary<string, string> GetStoreIdTokenConnectionParameters(string token)
        {
            var properties = GetExternalBrowserConnectionParameters();
            properties.Add("CLIENT_STORE_TEMPORARY_CREDENTIAL", "true");
            return properties;
        }

        public static Dictionary<string, string> GetOktaConnectionParameters()
        {
            var properties = GetBaseConnectionParameters();
            properties.Add("authenticator", Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_URL"));
            properties.Add("user", SsoUser);
            properties.Add("password", SsoPassword);

            return properties;
        }

        public static Dictionary<string, string> GetKeyPairFromFileContentParameters(String privateKey)
        {

            var properties = GetBaseConnectionParameters();
            properties.Add("authenticator", "snowflake_jwt");
            properties.Add("user", SsoUser);

            properties.Add("private_key", privateKey);

            return properties;
        }


        public static Dictionary<string, string> GetKeyPairFromFilePathParameters(String privateKeyPath)
        {

            var properties = GetBaseConnectionParameters();
            properties.Add("authenticator", "snowflake_jwt");
            properties.Add("user", AuthConnectionParameters.SsoUser);
            properties.Add("private_key_file", privateKeyPath);
            return properties;
        }
        public static String SetPrivateKeyFromFileContentParameters(Dictionary<string, string> param)
        {
            var config = String.Format("{0}authenticator={1};private_key={2};user={3}", SetBaseConnectionParameters(param), param["authenticator"], param["private_key"], SsoUser);
            return config;
        }

        public static String SetPrivateKeyFromFilePathParameters(Dictionary<string, string> param)
        {
            var config = String.Format("{0}authenticator={1};private_key_file={2};user={3}", SetBaseConnectionParameters(param),param["authenticator"], param["private_key_file"], SsoUser);
            return config;
        }

        public static String SetOktaConnectionParameters(Dictionary<string, string> param)
        {
            var config = String.Format("{0}authenticator={1};user={2};password={3}", SetBaseConnectionParameters(param), param["authenticator"], param["user"], param["password"]);
            return config;
        }

        public static string GetPriavteKeyContentForKeypairAuth(string envName)
        {
            string filePath = Environment.GetEnvironmentVariable(envName);
            Assert.IsNotNull(filePath);
            string pemKey = File.ReadAllText("../../../../" + filePath);
            Assert.IsNotNull(pemKey, $"Failed to read file: {filePath}");
            return pemKey;

        }

        public static string GetPriavteKeyPathForKeypairAuth(string envName)
        {
            string filePath = Environment.GetEnvironmentVariable(envName);
            Assert.IsNotNull(filePath);
            return Path.Combine("../../../../", filePath);
        }

        public static string GetOauthToken()
        {
            try
            {
                using (var client = new WebClient())
                {
                    var values = new NameValueCollection();
                    var authUrl = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_URL");
                    var clientId = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID");
                    var clientSecret = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET");
                    var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{clientId}:{clientSecret}"));

                    client.Headers[HttpRequestHeader.Authorization] = "Basic " + credentials;

                    values["username"] = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OKTA_USER");
                    values["password"] = Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_OKTA_PASS");
                    values["grant_type"] = "password";
                    values["scope"] = "session:role:" + Environment.GetEnvironmentVariable("SNOWFLAKE_AUTH_TEST_ROLE");

                    var fullResponse = Encoding.Default.GetString(client.UploadValues(authUrl, values));
                    var responseString = JObject.Parse(fullResponse);
                    Assert.IsNotNull(responseString["access_token"]);
                    Console.WriteLine(responseString.ToString());
                    return responseString["access_token"].ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }

        }
    }
}
