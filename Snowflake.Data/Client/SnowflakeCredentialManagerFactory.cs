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

        private static readonly object s_credentialManagerLock = new object();

        private static ISnowflakeCredentialManager s_credentialManager;
        private static bool s_isDefaultCredentialManager = true;
        private static ISnowflakeCredentialManager s_defaultCredentialManager;

        internal static string BuildCredentialKey(string host, string user, TokenType tokenType, string authenticator = null)
        {
            return $"{host.ToUpper()}:{user.ToUpper()}:{SFEnvironment.DriverName}:{tokenType.ToString().ToUpper()}:{authenticator?.ToUpper() ?? string.Empty}";
        }

        public static void UseDefaultCredentialManager()
        {
            SetCredentialManager(GetDefaultCredentialManager());
        }

        public static void UseInMemoryCredentialManager()
        {
            SetCredentialManager(SFCredentialManagerInMemoryImpl.Instance);
        }

        public static void UseFileCredentialManager()
        {
            SetCredentialManager(SFCredentialManagerFileImpl.Instance);
        }

        public static void UseWindowsCredentialManager()
        {
            SetCredentialManager(SFCredentialManagerWindowsNativeImpl.Instance);
        }

        public static void SetCredentialManager(ISnowflakeCredentialManager customCredentialManager)
        {
            lock (s_credentialManagerLock)
            {
                if (customCredentialManager == null)
                {
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                        "Credential manager cannot be null. If you want to use the default credential manager, please call the UseDefaultCredentialManager method.");
                }

                if (customCredentialManager == s_credentialManager) return;

                s_isDefaultCredentialManager = customCredentialManager == GetDefaultCredentialManager();
                s_logger.Info($"Setting the credential manager: {customCredentialManager.GetType().Name}");
                s_credentialManager = customCredentialManager;
            }
        }

        public static ISnowflakeCredentialManager GetCredentialManager()
        {
            if (s_credentialManager == null)
            {
                lock (s_credentialManagerLock)
                {
                    if (s_credentialManager == null)
                    {
                        s_isDefaultCredentialManager = true;
                        s_credentialManager = GetDefaultCredentialManager();
                    }
                }
            }
            var typeCredentialText = s_isDefaultCredentialManager ? "default" : "custom";
            s_logger.Info($"Using {typeCredentialText} credential manager: {s_credentialManager?.GetType().Name}");
            return s_credentialManager;
        }

        private static ISnowflakeCredentialManager GetDefaultCredentialManager()
        {
            if (s_defaultCredentialManager == null)
            {
                s_defaultCredentialManager = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                    ? (ISnowflakeCredentialManager)
                    SFCredentialManagerWindowsNativeImpl.Instance
                    : SFCredentialManagerInMemoryImpl.Instance;
            }

            return s_defaultCredentialManager;
        }
    }
}
