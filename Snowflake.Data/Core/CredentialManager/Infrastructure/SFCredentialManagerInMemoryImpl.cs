/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    using Snowflake.Data.Client;
    using Snowflake.Data.Log;
    using System.Collections.Generic;
    using System.Security;
    using Tools;

    internal class SFCredentialManagerInMemoryImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerInMemoryImpl>();

        private Dictionary<string, SecureString> s_credentials = new Dictionary<string, SecureString>();

        public static readonly SFCredentialManagerInMemoryImpl Instance = new SFCredentialManagerInMemoryImpl();

        public string GetCredentials(string key)
        {
            s_logger.Debug($"Getting credentials from memory for key: {key}");
            var hashKey = key.ToSha256Hash();
            if (s_credentials.TryGetValue(hashKey, out var secureToken))
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
            var hashKey = key.ToSha256Hash();
            s_logger.Debug($"Removing credentials from memory for key: {key}");
            s_credentials.Remove(hashKey);
        }

        public void SaveCredentials(string key, string token)
        {
            var hashKey = key.ToSha256Hash();
            s_logger.Debug($"Saving credentials into memory for key: {hashKey}");
            s_credentials[hashKey] = SecureStringHelper.Encode(token);
        }
    }
}
