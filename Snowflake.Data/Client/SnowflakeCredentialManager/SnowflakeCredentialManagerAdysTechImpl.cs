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

        public string GetCredentials(string key)
        {
            try
            {
                var networkCredentials = CredentialManager.GetCredentials(key);
                return networkCredentials.Password;
            }
            catch (NullReferenceException)
            {
                s_logger.Info("Unable to get credentials for the specified key");
                return "";
            }
        }

        public void RemoveCredentials(string key)
        {
            try
            {
                CredentialManager.RemoveCredentials(key);
            }
            catch (CredentialAPIException)
            {
                s_logger.Info("Unable to remove credentials because the specified key did not exist in the credential manager");
            }
        }

        public void SaveCredentials(string key, string token)
        {
            var networkCredentials = new NetworkCredential(key, token);
            CredentialManager.SaveCredentials(key, networkCredentials);
        }
    }
}
