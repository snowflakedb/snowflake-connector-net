namespace Snowflake.Data.Configuration;

internal readonly record struct EnvVar<T>
{
    public readonly string Name;
    public readonly T DefaultValue;

    public EnvVar(string name, T defaultValue)
    {
        Name = name;
        DefaultValue = defaultValue;
    }
};

internal static class EnvVars
{
    internal static readonly EnvVar<bool> DisableMinicore = new("SF_DISABLE_MINICORE",  false);
    internal static readonly EnvVar<bool> EnableAwsWifOutboundToken = new("SNOWFLAKE_ENABLE_AWS_WIF_OUTBOUND_TOKEN",  false);
    internal static readonly EnvVar<bool> SkipTokenFilePermissionsVerification = new("SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION",  false);
    internal static readonly EnvVar<bool> SkipWarnForFilePermissionsVerification = new("SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE",  false);
    internal static readonly EnvVar<bool> DisablePlatformDetection = new("SNOWFLAKE_DISABLE_PLATFORM_DETECTION", false);
    internal static readonly EnvVar<int> CrlValidityTime = new("SF_CRL_VALIDITY_TIME",  1);
    internal static readonly EnvVar<int> CrlCacheRemovalDelay = new("SF_CRL_CACHE_REMOVAL_DELAY",  7);

    internal static readonly EnvVar<string> SnowflakeHome = new("SNOWFLAKE_HOME", string.Empty);
    internal static readonly EnvVar<string> DefaultConnectionName = new("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "default");
    internal static readonly EnvVar<string> WifEndpoint = new("IDENTITY_ENDPOINT",  string.Empty);
    internal static readonly EnvVar<string> WifHeader = new("IDENTITY_HEADER",  string.Empty);
    internal static readonly EnvVar<string> WifClientId = new("MANAGED_IDENTITY_CLIENT_ID",  string.Empty);
    internal static readonly EnvVar<string> RunningInsideSpcs = new("SNOWFLAKE_RUNNING_INSIDE_SPCS",  string.Empty);
    internal static readonly EnvVar<string> ClientConfigFile = new("SF_CLIENT_CONFIG_FILE",  string.Empty);
    internal static readonly EnvVar<string> TemporaryCredentialDir = new("SF_TEMPORARY_CREDENTIAL_CACHE_DIR",  string.Empty);
    internal static readonly EnvVar<string> CommonCacheDirectory = new("XDG_CACHE_HOME",  string.Empty);
    internal static readonly EnvVar<string> Home = new("HOME", string.Empty);

    // Platform detection env vars
    internal static readonly EnvVar<string> LambdaTaskRoot = new("LAMBDA_TASK_ROOT", string.Empty);
    internal static readonly EnvVar<string> GithubActions = new("GITHUB_ACTIONS", string.Empty);
    internal static readonly EnvVar<string> FunctionsWorkerRuntime = new("FUNCTIONS_WORKER_RUNTIME", string.Empty);
    internal static readonly EnvVar<string> FunctionsExtensionVersion = new("FUNCTIONS_EXTENSION_VERSION", string.Empty);
    internal static readonly EnvVar<string> AzureWebJobsStorage = new("AzureWebJobsStorage", string.Empty);
    internal static readonly EnvVar<string> KService = new("K_SERVICE", string.Empty);
    internal static readonly EnvVar<string> KRevision = new("K_REVISION", string.Empty);
    internal static readonly EnvVar<string> KConfiguration = new("K_CONFIGURATION", string.Empty);
    internal static readonly EnvVar<string> CloudRunJob = new("CLOUD_RUN_JOB", string.Empty);
    internal static readonly EnvVar<string> CloudRunExecution = new("CLOUD_RUN_EXECUTION", string.Empty);
}

