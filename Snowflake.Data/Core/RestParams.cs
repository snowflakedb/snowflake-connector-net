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
    }

    internal static class RestPath
    {
        internal const string SF_SESSION_PATH = "/session";

        internal const string SF_LOGIN_PATH = SF_SESSION_PATH + "/v1/login-request";

        internal const string SF_TOKEN_REQUEST_PATH = SF_SESSION_PATH + "/token-request";

        internal const string SF_AUTHENTICATOR_REQUEST_PATH = SF_SESSION_PATH + "/authenticator-request";
    }

    internal class SFEnvironment
    {
        static SFEnvironment()
        {
            ClientEnv = new LoginRequestClientEnv()
            {
                application = System.Diagnostics.Process.GetCurrentProcess().ProcessName,
                osVersion = System.Environment.OSVersion.VersionString,
#if NET46
                netRuntime = "CLR:" + Environment.Version.ToString()
#else
                netRuntime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription
#endif
            };

            DriverVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();
            DriverName = ".NET";
        }

        internal static string DriverName { get; private set; }
        internal static string DriverVersion { get; private set; }
        internal static LoginRequestClientEnv ClientEnv { get; private set; }
    }
}
