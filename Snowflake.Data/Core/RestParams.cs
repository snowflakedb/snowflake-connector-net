using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Snowflake.Data.Core
{
    internal static class RestParams
    {
        internal const string SF_QUERY_WAREHOUSE = "warehouse";

        internal const string SF_QUERY_DB = "databaseName";

        internal const string SF_QUERY_SCHEMA = "schemaName";

        internal const string SF_QUERY_ROLE = "roleName";

        internal const string SF_QUERY_REQUEST_ID = "requestId";

        internal const string SF_QUERY_REQUEST_GUID = "request_guid";

        internal const string SF_QUERY_START_TIME = "clientStartTime";

        internal const string SF_QUERY_RETRY_COUNT = "retryCount";

        internal const string SF_QUERY_RETRY_REASON = "retryReason";

        internal const string SF_QUERY_SESSION_DELETE = "delete";
    }

    internal static class RestPath
    {
        internal const string SF_SESSION_PATH = "/session";

        internal const string SF_LOGIN_PATH = SF_SESSION_PATH + "/v1/login-request";

        internal const string SF_TOKEN_REQUEST_PATH = SF_SESSION_PATH + "/token-request";

        internal const string SF_AUTHENTICATOR_REQUEST_PATH = SF_SESSION_PATH + "/authenticator-request";

        internal const string SF_QUERY_PATH = "/queries/v1/query-request";

        internal const string SF_MONITOR_QUERY_PATH = "/monitoring/queries/";

        internal const string SF_SESSION_HEARTBEAT_PATH = SF_SESSION_PATH + "/heartbeat";

        internal const string SF_CONSOLE_LOGIN = "/console/login";
    }

    internal static class OAuthFlowConfig
    {
        internal const string SnowflakeAuthorizeUrl = "/oauth/authorize";
        internal const string SnowflakeTokenUrl = "/oauth/token-request";
        internal const string DefaultScopePrefixBeforeRole = "session:role:";
    }

    internal static class OktaUrl
    {
        internal const string DOMAIN = "okta.com";
        internal const string SSO_SAML_PATH = "/sso/saml";
    }

    internal class SFEnvironment
    {
        static SFEnvironment()
        {
            ClientEnv = new LoginRequestClientEnv()
            {
                processName = System.Diagnostics.Process.GetCurrentProcess().ProcessName,
                osVersion = Environment.OSVersion.VersionString,
                netRuntime = ExtractRuntime(),
                netVersion = ExtractVersion(),
                applicationPath = ExtractApplicationPath(),
            };

            DriverVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString();
            DriverName = ".NET";
        }

        //temporary change for pretend as ODBC
        internal static string DriverName { get; set; }
        internal static string DriverVersion { get; set; }
        internal static LoginRequestClientEnv ClientEnv { get; private set; }

        internal static string ExtractRuntime()
        {
            return RuntimeInformation.FrameworkDescription.Substring(0, RuntimeInformation.FrameworkDescription.LastIndexOf(' ')).Replace(" ", "");
        }

        internal static string ExtractVersion()
        {
            var version = RuntimeInformation.FrameworkDescription.Substring(RuntimeInformation.FrameworkDescription.LastIndexOf(' ')).Replace(" ", "");
            int secondPeriodIndex = version.IndexOf('.', version.IndexOf('.') + 1);
            return version.Substring(0, secondPeriodIndex);
        }

        internal static string ExtractApplicationPath()
        {
            try
            {
                var assembly = Assembly.GetEntryAssembly();
                if (assembly != null && !string.IsNullOrEmpty(assembly.Location))
                {
                    return assembly.Location;
                }
                
                var process = System.Diagnostics.Process.GetCurrentProcess();
                var mainModule = process.MainModule;
                if (mainModule != null && !string.IsNullOrEmpty(mainModule.FileName))
                {
                    return mainModule.FileName;
                }

                return "UNKNOWN";
            }
            catch (Exception)
            {
                return "UNKNOWN";
            }
        }
    }
}
