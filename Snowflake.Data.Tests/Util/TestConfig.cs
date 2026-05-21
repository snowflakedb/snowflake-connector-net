using Newtonsoft.Json;

namespace Snowflake.Data.Tests;

public class TestConfig
{
    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_USER", NullValueHandling = NullValueHandling.Ignore)]
    internal string user { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PASSWORD", NullValueHandling = NullValueHandling.Ignore)]
    internal string password { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_ACCOUNT", NullValueHandling = NullValueHandling.Ignore)]
    internal string account { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_HOST", NullValueHandling = NullValueHandling.Ignore)]
    internal string host { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PORT", NullValueHandling = NullValueHandling.Ignore)]
    internal string port { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_WAREHOUSE", NullValueHandling = NullValueHandling.Ignore)]
    internal string warehouse { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_DATABASE", NullValueHandling = NullValueHandling.Ignore)]
    internal string database { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_SCHEMA", NullValueHandling = NullValueHandling.Ignore)]
    internal string schema { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_ROLE", NullValueHandling = NullValueHandling.Ignore)]
    internal string role { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PROTOCOL", NullValueHandling = NullValueHandling.Ignore)]
    internal string protocol { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_AUTHENTICATOR", NullValueHandling = NullValueHandling.Ignore)]
    internal string authenticator { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_USER", NullValueHandling = NullValueHandling.Ignore)]
    internal string oktaUser { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_PASSWORD", NullValueHandling = NullValueHandling.Ignore)]
    internal string oktaPassword { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OKTA_URL", NullValueHandling = NullValueHandling.Ignore)]
    internal string oktaUrl { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_JWT_USER", NullValueHandling = NullValueHandling.Ignore)]
    internal string jwtAuthUser { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PEM_FILE", NullValueHandling = NullValueHandling.Ignore)]
    internal string pemFilePath { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_P8_FILE", NullValueHandling = NullValueHandling.Ignore)]
    internal string p8FilePath { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PWD_PROTECTED_PK_FILE", NullValueHandling = NullValueHandling.Ignore)]
    internal string pwdProtectedPrivateKeyFilePath { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PK_CONTENT", NullValueHandling = NullValueHandling.Ignore)]
    internal string privateKey { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PROTECTED_PK_CONTENT", NullValueHandling = NullValueHandling.Ignore)]
    internal string pwdProtectedPrivateKey { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PK_PWD", NullValueHandling = NullValueHandling.Ignore)]
    internal string privateKeyFilePwd { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_TOKEN", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthToken { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_EXP_OAUTH_TOKEN", NullValueHandling = NullValueHandling.Ignore)]
    internal string expOauthToken { get; set; }

    [JsonProperty(PropertyName = "PROXY_HOST", NullValueHandling = NullValueHandling.Ignore)]
    internal string proxyHost { get; set; }

    [JsonProperty(PropertyName = "PROXY_PORT", NullValueHandling = NullValueHandling.Ignore)]
    internal string proxyPort { get; set; }

    [JsonProperty(PropertyName = "AUTH_PROXY_HOST", NullValueHandling = NullValueHandling.Ignore)]
    internal string authProxyHost { get; set; }

    [JsonProperty(PropertyName = "AUTH_PROXY_PORT", NullValueHandling = NullValueHandling.Ignore)]
    internal string authProxyPort { get; set; }

    [JsonProperty(PropertyName = "AUTH_PROXY_USER", NullValueHandling = NullValueHandling.Ignore)]
    internal string authProxyUser { get; set; }

    [JsonProperty(PropertyName = "AUTH_PROXY_PWD", NullValueHandling = NullValueHandling.Ignore)]
    internal string authProxyPwd { get; set; }

    [JsonProperty(PropertyName = "NON_PROXY_HOSTS", NullValueHandling = NullValueHandling.Ignore)]
    internal string nonProxyHosts { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_CLIENT_ID", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthClientId { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthClientSecret { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_SCOPE", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthScope { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_REDIRECT_URI", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthRedirectUri { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_AUTHORIZATION_URL", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthAuthorizationUrl { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_OAUTH_TOKEN_REQUEST_URL", NullValueHandling = NullValueHandling.Ignore)]
    internal string oauthTokenRequestUrl { get; set; }

    [JsonProperty(PropertyName = "SNOWFLAKE_TEST_PROGRAMMATIC_ACCESS_TOKEN", NullValueHandling = NullValueHandling.Ignore)]
    internal string programmaticAccessToken { get; set; }

    public TestConfig()
    {
        protocol = "https";
        port = "443";
    }
}
