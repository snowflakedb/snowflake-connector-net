/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Client
{
    using System;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.CredentialManager;
    using Snowflake.Data.Core.CredentialManager.Infrastructure;
    using Snowflake.Data.Log;
    using System.Runtime.InteropServices;

    public class SFCredentialManagerFactory
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerFactory>();

        private static ISnowflakeCredentialManager s_customCredentialManager = null;

        internal static string BuildCredentialKey(string host, string user, TokenType tokenType, string authenticator = null)
        {
            return $"{host.ToUpper()}:{user.ToUpper()}:{SFEnvironment.DriverName}:{tokenType.ToString().ToUpper()}:{authenticator?.ToUpper() ?? string.Empty}";
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

        public static ISnowflakeCredentialManager GetCredentialManager()
        {
            if (s_customCredentialManager == null)
            {
                var defaultCredentialManager = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? (ISnowflakeCredentialManager)
                    SFCredentialManagerWindowsNativeImpl.Instance : SFCredentialManagerInMemoryImpl.Instance;
                s_logger.Info($"Using the default credential manager: {defaultCredentialManager.GetType().Name}");
                return defaultCredentialManager;
            }
            s_logger.Info($"Using a custom credential manager: {s_customCredentialManager.GetType().Name}");
            return s_customCredentialManager;
        }
    }
}
