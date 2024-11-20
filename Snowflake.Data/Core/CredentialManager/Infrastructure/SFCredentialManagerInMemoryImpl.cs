/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */


using System;
using System.Collections.Generic;
using System.Security;
using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.CredentialManager.Infrastructure
{
    internal class SFCredentialManagerInMemoryImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerInMemoryImpl>();

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        private Dictionary<string, SecureString> s_credentials = new Dictionary<string, SecureString>();

        public static readonly SFCredentialManagerInMemoryImpl Instance = new SFCredentialManagerInMemoryImpl();

        public string GetCredentials(string key)
        {
            try
            {
                _lock.EnterReadLock();
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
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void RemoveCredentials(string key)
        {
            try
            {
                _lock.EnterWriteLock();
                s_logger.Debug($"Removing credentials from memory for key: {key}");
                s_credentials.Remove(key);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void SaveCredentials(string key, string token)
        {
            try
            {
                _lock.EnterWriteLock();
                s_logger.Debug($"Saving credentials into memory for key: {key}");
                s_credentials[key] = SecureStringHelper.Encode(token);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
    }
}
