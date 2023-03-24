using System;
using System.Collections.Generic;
using System.Text;

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

        internal const string SF_QUERY_SESSION_DELETE = "delete";
    }

    internal static class RestPath
    {
        internal const string SF_SESSION_PATH = "/session";

        internal const string SF_LOGIN_PATH = SF_SESSION_PATH + "/v1/login-request";

        internal const string SF_TOKEN_REQUEST_PATH = SF_SESSION_PATH + "/token-request";

        internal const string SF_AUTHENTICATOR_REQUEST_PATH = SF_SESSION_PATH + "/authenticator-request";

        internal const string SF_QUERY_PATH = "/queries/v1/query-request";

        internal const string SF_SESSION_HEARTBEAT_PATH = SF_SESSION_PATH + "/heartbeat";
    }

    internal class SFEnvironment
    {
        static SFEnvironment()
        {
            ClientEnv = new LoginRequestClientEnv()
            {
                application = System.Diagnostics.Process.GetCurrentProcess().ProcessName,
                osVersion = System.Environment.OSVersion.VersionString,
#if NETFRAMEWORK
                netRuntime = "NETFramework",
                netVersion = "4.7.1",
#else
                netRuntime = "NETCore",
                netVersion ="2.0",
#endif
            };

            DriverVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();
            DriverName = ".NET";
        }

        //temporary change for pretend as ODBC
        internal static string DriverName { get; set; }
        internal static string DriverVersion { get; set; }
        internal static LoginRequestClientEnv ClientEnv { get; private set; }
    }
}
