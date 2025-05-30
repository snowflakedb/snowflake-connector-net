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
            s_logger.Debug($"Getting credentials from memory for key: {key}");
            bool found;
            SecureString secureToken;
            _lock.EnterReadLock();
            try
            {
                found = s_credentials.TryGetValue(key, out secureToken);
            }
            finally
            {
                _lock.ExitReadLock();
            }
            if (found)
            {
                return SecureStringHelper.Decode(secureToken);
            }
            s_logger.Debug("Unable to get credentials for the specified key");
            return "";
        }

        public void RemoveCredentials(string key)
        {
            s_logger.Debug($"Removing credentials from memory for key: {key}");
            _lock.EnterWriteLock();
            try
            {
                s_credentials.Remove(key);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void SaveCredentials(string key, string token)
        {
            s_logger.Debug($"Saving credentials into memory for key: {key}");
            var secureToken = SecureStringHelper.Encode(token);
            _lock.EnterWriteLock();
            try
            {
                s_credentials[key] = secureToken;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        internal int GetCount()
        {
            _lock.EnterReadLock();
            try
            {
                return s_credentials.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }
}
