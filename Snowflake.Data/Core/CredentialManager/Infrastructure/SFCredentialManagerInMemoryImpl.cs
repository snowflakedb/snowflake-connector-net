/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */


using System.Collections.Generic;
using System.Security;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class SFCredentialManagerInMemoryImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerInMemoryImpl>();

        private Dictionary<string, SecureString> s_credentials = new Dictionary<string, SecureString>();

        public static readonly SFCredentialManagerInMemoryImpl Instance = new SFCredentialManagerInMemoryImpl();

        public string GetCredentials(string key)
        {
            s_logger.Debug($"Getting credentials from memory for key: {key}");
            if (s_credentials.TryGetValue(key, out var secureToken))
            {
                return SecureStringHelper.Decode(secureToken);
            }
            else
            {
                s_logger.Info("Unable to get credentials for the specified key");
                return "";
            }
        }

        public void RemoveCredentials(string key)
        {
            s_logger.Debug($"Removing credentials from memory for key: {key}");
            s_credentials.Remove(key);
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving credentials into memory for key: {key}");
            s_credentials[key] = SecureStringHelper.Encode(token);
        }
    }
}
