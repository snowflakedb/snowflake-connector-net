/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Runtime.InteropServices;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerFactory
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerFactory>();

        private static readonly object s_credentialManagerLock = new object();
        private static readonly ISnowflakeCredentialManager s_defaultCredentialManager = GetDefaultCredentialManager();

        private static ISnowflakeCredentialManager s_credentialManager;

        internal static string GetSecureCredentialKey(string host, string user, TokenType tokenType)
        {
            return $"{host.ToUpper()}:{user.ToUpper()}:{tokenType.ToString().ToUpper()}".ToSha256Hash();
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
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                throw new Exception("File credential manager implementation is not supported on Windows");
            }
            SetCredentialManager(SFCredentialManagerFileImpl.Instance);
        }

        public static void UseWindowsCredentialManager()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                throw new Exception("Windows native credential manager implementation can be used only on Windows");
            }
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

                if (customCredentialManager == s_credentialManager)
                {
                    s_logger.Info($"Credential manager is already set to: {customCredentialManager.GetType().Name}");
                    return;
                }

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
                        s_credentialManager = s_defaultCredentialManager;
                    }
                }
            }

            var credentialManager = s_credentialManager;
            var typeCredentialText = credentialManager == s_defaultCredentialManager ? "default" : "custom";
            s_logger.Info($"Using {typeCredentialText} credential manager: {credentialManager?.GetType().Name}");
            return credentialManager;
        }

        private static ISnowflakeCredentialManager GetDefaultCredentialManager()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? (ISnowflakeCredentialManager)
                SFCredentialManagerWindowsNativeImpl.Instance
                : SFCredentialManagerFileImpl.Instance;
        }
    }
}