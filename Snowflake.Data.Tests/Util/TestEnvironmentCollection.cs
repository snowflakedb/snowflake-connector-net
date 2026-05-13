using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Newtonsoft.Json;
using Xunit;

namespace Snowflake.Data.Tests;

[CollectionDefinition(TestEnvironmentCollectionName)]
public class TestEnvironmentCollection : ICollectionFixture<TestEnvironmentFixture>
{
    public const string TestEnvironmentCollectionName = "TestEnvironment";
}

/// <summary>
/// Reads configuration and records performance
/// </summary>
public class TestEnvironmentFixture : IDisposable
{
    public TestConfig TestConfig { get; private set; }

    private readonly ConcurrentDictionary<string, TimeSpan> _testPerformance;

    public void RecordTestPerformance(string name, TimeSpan time)
    {
        _testPerformance.AddOrUpdate(name, time, (_, _) => time);
    }

    protected TestEnvironmentFixture()
    {
        var cloud = Environment.GetEnvironmentVariable("snowflake_cloud_env");
        Assert.True(cloud is null or "AWS" or "AZURE" or "GCP", $"{cloud} is not supported. Specify AWS, AZURE or GCP as cloud environment");

        TestConfig = ReadTestConfig();
        _testPerformance = new ConcurrentDictionary<string, TimeSpan>();

#if NETFRAMEWORK
            log4net.GlobalContext.Properties["framework"] = "net471";
            log4net.Config.XmlConfigurator.Configure();
#else
        log4net.GlobalContext.Properties["framework"] = "net10.0";
        var logRepository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
        log4net.Config.XmlConfigurator.Configure(logRepository, new FileInfo("App.config"));
#endif

        // A lot of blocking code + async mixup does not play along well with standard thread pool allocation algo
        ThreadPool.SetMinThreads(100, 100);
    }

    public void Dispose()
    {
        CreateTestTimeArtifact();
    }

    private static TestConfig ReadTestConfig()
    {
        var fileName = "parameters.json";
        var testConfig = File.Exists(fileName) ? ReadTestConfigFile(fileName) : ReadTestConfigEnvVariables();
        var uniqueSuffix = Guid.NewGuid().ToString().Replace("-", "_");
        testConfig.schema = $"{testConfig.schema}_{uniqueSuffix}";
        return testConfig;
    }

    private static TestConfig ReadTestConfigEnvVariables()
    {
        var config = new TestConfig();
        config.user = ReadEnvVariableIfSet(config.user, "SNOWFLAKE_TEST_USER");
        config.password = ReadEnvVariableIfSet(config.password, "SNOWFLAKE_TEST_PASSWORD");
        config.account = ReadEnvVariableIfSet(config.account, "SNOWFLAKE_TEST_ACCOUNT");
        config.host = ReadEnvVariableIfSet(config.host, "SNOWFLAKE_TEST_HOST");
        config.port = ReadEnvVariableIfSet(config.port, "SNOWFLAKE_TEST_PORT");
        config.warehouse = ReadEnvVariableIfSet(config.warehouse, "SNOWFLAKE_TEST_WAREHOUSE");
        config.database = ReadEnvVariableIfSet(config.database, "SNOWFLAKE_TEST_DATABASE");
        config.schema = ReadEnvVariableIfSet(config.schema, "SNOWFLAKE_TEST_SCHEMA");
        config.role = ReadEnvVariableIfSet(config.role, "SNOWFLAKE_TEST_ROLE");
        config.protocol = ReadEnvVariableIfSet(config.protocol, "SNOWFLAKE_TEST_PROTOCOL");
        config.authenticator = ReadEnvVariableIfSet(config.authenticator, "SNOWFLAKE_TEST_AUTHENTICATOR");
        config.oktaUser = ReadEnvVariableIfSet(config.oktaUser, "SNOWFLAKE_TEST_OKTA_USER");
        config.oktaPassword = ReadEnvVariableIfSet(config.oktaPassword, "SNOWFLAKE_TEST_OKTA_PASSWORD");
        config.oktaUrl = ReadEnvVariableIfSet(config.oktaUrl, "SNOWFLAKE_TEST_OKTA_URL");
        config.jwtAuthUser = ReadEnvVariableIfSet(config.jwtAuthUser, "SNOWFLAKE_TEST_JWT_USER");
        config.pemFilePath = ReadEnvVariableIfSet(config.pemFilePath, "SNOWFLAKE_TEST_PEM_FILE");
        config.p8FilePath = ReadEnvVariableIfSet(config.p8FilePath, "SNOWFLAKE_TEST_P8_FILE");
        config.pwdProtectedPrivateKeyFilePath =
            ReadEnvVariableIfSet(config.pwdProtectedPrivateKeyFilePath, "SNOWFLAKE_TEST_PWD_PROTECTED_PK_FILE");
        config.privateKey = ReadEnvVariableIfSet(config.privateKey, "SNOWFLAKE_TEST_PK_CONTENT");
        config.pwdProtectedPrivateKey = ReadEnvVariableIfSet(config.pwdProtectedPrivateKey, "SNOWFLAKE_TEST_PROTECTED_PK_CONTENT");
        config.privateKeyFilePwd = ReadEnvVariableIfSet(config.privateKeyFilePwd, "SNOWFLAKE_TEST_PK_PWD");
        config.oauthToken = ReadEnvVariableIfSet(config.oauthToken, "SNOWFLAKE_TEST_OAUTH_TOKEN");
        config.expOauthToken = ReadEnvVariableIfSet(config.expOauthToken, "SNOWFLAKE_TEST_EXP_OAUTH_TOKEN");
        config.proxyHost = ReadEnvVariableIfSet(config.proxyHost, "SNOWFLAKE_TEST_PROXY_HOST");
        config.proxyPort = ReadEnvVariableIfSet(config.proxyPort, "SNOWFLAKE_TEST_PROXY_PORT");
        config.authProxyHost = ReadEnvVariableIfSet(config.authProxyHost, "SNOWFLAKE_TEST_AUTH_PROXY_HOST");
        config.authProxyPort = ReadEnvVariableIfSet(config.authProxyPort, "SNOWFLAKE_TEST_AUTH_PROXY_PORT");
        config.authProxyUser = ReadEnvVariableIfSet(config.authProxyUser, "SNOWFLAKE_TEST_AUTH_PROXY_USER");
        config.authProxyPwd = ReadEnvVariableIfSet(config.authProxyPwd, "SNOWFLAKE_TEST_AUTH_PROXY_PWD");
        config.nonProxyHosts = ReadEnvVariableIfSet(config.nonProxyHosts, "SNOWFLAKE_TEST_NON_PROXY_HOSTS");
        config.oauthClientId = ReadEnvVariableIfSet(config.oauthClientId, "SNOWFLAKE_TEST_OAUTH_CLIENT_ID");
        config.oauthClientSecret = ReadEnvVariableIfSet(config.oauthClientSecret, "SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET");
        config.oauthScope = ReadEnvVariableIfSet(config.oauthScope, "SNOWFLAKE_TEST_OAUTH_SCOPE");
        config.oauthRedirectUri = ReadEnvVariableIfSet(config.oauthRedirectUri, "SNOWFLAKE_TEST_OAUTH_REDIRECT_URI");
        config.oauthAuthorizationUrl = ReadEnvVariableIfSet(config.oauthAuthorizationUrl, "SNOWFLAKE_TEST_OAUTH_AUTHORIZATION_URL");
        config.oauthTokenRequestUrl = ReadEnvVariableIfSet(config.oauthTokenRequestUrl, "SNOWFLAKE_TEST_OAUTH_TOKEN_REQUEST_URL");
        config.programmaticAccessToken = ReadEnvVariableIfSet(config.programmaticAccessToken, "SNOWFLAKE_TEST_PROGRAMMATIC_ACCESS_TOKEN");
        return config;
    }

    private static string ReadEnvVariableIfSet(string defaultValue, string variableName)
    {
        var variableValue = Environment.GetEnvironmentVariable(variableName);
        return string.IsNullOrEmpty(variableValue) ? defaultValue : variableValue;
    }

    private static TestConfig ReadTestConfigFile(string fileName)
    {
        var reader = new StreamReader(fileName);
        var testConfigString = reader.ReadToEnd();
        var testConfigs = JsonConvert.DeserializeObject<Dictionary<string, TestConfig>>(testConfigString);
        if (testConfigs.TryGetValue("testconnection", out var testConnectionConfig))
            return testConnectionConfig;

        Assert.Fail("Failed to load test configuration");
        throw new Exception("Failed to load test configuration");
    }

    private void CreateTestTimeArtifact()
    {
        if (_testPerformance == null || _testPerformance.Count == 0)
            return;

        var performance = string.Join("\n", _testPerformance.Select(test => $"{test.Key};{Math.Round(test.Value.TotalMilliseconds, 0)}"));
        var resultText = $"test;time_in_ms\n{performance}";

        var dotnetVersion = Environment.GetEnvironmentVariable("net_version");
        var cloudEnv = Environment.GetEnvironmentVariable("snowflake_cloud_env");

        var separator = Path.DirectorySeparatorChar;

        // We have to go up 3 times as the working directory path looks as follows:
        // Snowflake.Data.Tests/bin/debug/{.net_version}/
        File.WriteAllText($"..{separator}..{separator}..{separator}{GetOs()}_{dotnetVersion}_{cloudEnv}_performance.csv", resultText);
    }

    private static string GetOs()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return "windows";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return "linux";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return "macos";
        }

        return "unknown";
    }
}
