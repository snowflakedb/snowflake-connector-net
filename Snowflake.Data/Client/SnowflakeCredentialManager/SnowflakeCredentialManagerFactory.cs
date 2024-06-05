/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core;
using Snowflake.Data.Log;
using System.Runtime.InteropServices;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerFactory
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerFactory>();

        private static ISnowflakeCredentialManager s_customCredentialManager = null;

        internal static string BuildCredentialKey(string host, string user, string tokenType)
        {
            return $"{host.ToUpper()}:{user.ToUpper()}:{SFEnvironment.DriverName}:{tokenType.ToUpper()}";
        }

        public static void UseDefaultCredentialManager()
        {
            s_logger.Info("Clearing the custom credential manager");
            s_customCredentialManager = null;
        }

        public static void SetCredentialManager(ISnowflakeCredentialManager customCredentialManager)
        {
            s_logger.Info($"Setting the custom credential manager: {customCredentialManager.GetType().Name}");
            s_customCredentialManager = customCredentialManager;
        }

        internal static ISnowflakeCredentialManager GetCredentialManager()
        {
            if (s_customCredentialManager == null)
            {
                s_logger.Info("Using the default credential manager");
                return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? (ISnowflakeCredentialManager)
                    SnowflakeCredentialManagerAdysTechImpl.Instance : SnowflakeCredentialManagerInMemoryImpl.Instance;
            }
            else
            {
                s_logger.Info("Using a custom credential manager");
                return s_customCredentialManager;
            }
        }
    }
}
