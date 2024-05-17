/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using AdysTech.CredentialManager;
using Snowflake.Data.Log;
using System;
using System.Net;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerAdysTechImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerAdysTechImpl>();

        public static readonly SnowflakeCredentialManagerAdysTechImpl Instance = new SnowflakeCredentialManagerAdysTechImpl();

        public string GetCredentials(string key)
        {
            try
            {
                s_logger.Debug($"Getting the credentials for key: {key}");
                var networkCredentials = CredentialManager.GetCredentials(key);
                return networkCredentials.Password;
            }
            catch (NullReferenceException)
            {
                s_logger.Info($"Unable to get credentials for key: {key}");
                return "";
            }
        }

        public void RemoveCredentials(string key)
        {
            try
            {
                s_logger.Debug($"Removing the credentials for key: {key}");
                CredentialManager.RemoveCredentials(key);
            }
            catch (CredentialAPIException)
            {
                s_logger.Info($"Unable to remove credentials because the specified key did not exist: {key}");
            }
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving the credentials for key: {key}");
            var networkCredentials = new NetworkCredential(key, token);
            CredentialManager.SaveCredentials(key, networkCredentials);
        }
    }
}
