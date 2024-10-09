/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System.Runtime.InteropServices;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerFactory
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerFactory>();

        private static readonly object credentialManagerLock = new object();

        private static ISnowflakeCredentialManager s_customCredentialManager = null;

        internal static string BuildCredentialKey(string host, string user, TokenType tokenType, string authenticator = null)
        {
            return $"{host.ToUpper()}:{user.ToUpper()}:{SFEnvironment.DriverName}:{tokenType.ToString().ToUpper()}:{authenticator?.ToUpper() ?? string.Empty}";
        }

        public static void UseDefaultCredentialManager()
        {
            lock (credentialManagerLock)
            {
                s_logger.Info("Clearing the custom credential manager");
                s_customCredentialManager = null;
            }
        }

        public static void SetCredentialManager(ISnowflakeCredentialManager customCredentialManager)
        {
            lock (credentialManagerLock)
            {
                s_logger.Info($"Setting the custom credential manager: {customCredentialManager.GetType().Name}");
                s_customCredentialManager = customCredentialManager;
            }
        }

        public static ISnowflakeCredentialManager GetCredentialManager()
        {

            if (s_customCredentialManager == null)
            {
                lock (credentialManagerLock)
                {
                    if (s_customCredentialManager == null)
                    {
                        var defaultCredentialManager = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? (ISnowflakeCredentialManager)
                            SFCredentialManagerWindowsNativeImpl.Instance : SFCredentialManagerInMemoryImpl.Instance;
                        s_logger.Info($"Using the default credential manager: {defaultCredentialManager.GetType().Name}");
                        return defaultCredentialManager;
                    }
                }
            }
            s_logger.Info($"Using a custom credential manager: {s_customCredentialManager.GetType().Name}");
            return s_customCredentialManager;
        }
    }
}
