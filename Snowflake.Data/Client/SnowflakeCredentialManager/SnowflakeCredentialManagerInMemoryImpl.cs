/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System.Collections.Generic;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerInMemoryImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerInMemoryImpl>();

        private Dictionary<string, string> s_credentials = new Dictionary<string, string>();

        public static readonly SnowflakeCredentialManagerInMemoryImpl Instance = new SnowflakeCredentialManagerInMemoryImpl();

        public string GetCredentials(string key)
        {
            s_logger.Debug($"Getting credentials from memory for key: {key}");
            string token;
            if (s_credentials.TryGetValue(key, out token))
            {
                return token;
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
            s_credentials[key] = token;
        }
    }
}
