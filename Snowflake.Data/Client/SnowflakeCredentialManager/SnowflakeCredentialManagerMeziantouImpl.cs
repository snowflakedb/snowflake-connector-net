/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Meziantou.Framework.Win32;
using Snowflake.Data.Log;
using System;
using System.ComponentModel;

namespace Snowflake.Data.Client
{
    public class SnowflakeCredentialManagerMeziantouImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerMeziantouImpl>();

        public static readonly SnowflakeCredentialManagerMeziantouImpl Instance = new SnowflakeCredentialManagerMeziantouImpl();

        public string GetCredentials(string key)
        {
            try
            {
                s_logger.Debug($"Getting the credentials for key: {key}");
                var networkCredentials = CredentialManager.ReadCredential(key);
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
                CredentialManager.DeleteCredential(key);
            }
            catch (Win32Exception)
            {
                s_logger.Info($"Unable to remove credentials because the specified key did not exist: {key}");
            }
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving the credentials for key: {key}");
            CredentialManager.WriteCredential(key, key, token, CredentialPersistence.LocalMachine);
        }
    }
}
