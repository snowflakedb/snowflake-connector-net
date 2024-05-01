/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Snowflake.Data.Client
{
    public struct EncryptedToken
    {
        public byte[] IV;
        public byte[] Key;
        public byte[] Bytes;
    }

    internal class SnowflakeCredentialEncryption
    {
        internal static EncryptedToken EncryptToken(string token)
        {
            EncryptedToken encryptedToken;

            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.BlockSize = 128;
                aes.GenerateKey();
                encryptedToken.Key = aes.Key;
                aes.GenerateIV();
                encryptedToken.IV = aes.IV;

                using (var encryptor = aes.CreateEncryptor())
                {
                    var tokenBytes = Encoding.UTF8.GetBytes(token);
                    encryptedToken.Bytes = encryptor.TransformFinalBlock(tokenBytes, 0, tokenBytes.Length);
                }
            }

            return encryptedToken;
        }

        internal static string DecryptToken(EncryptedToken encryptedToken)
        {
            using (var aes = Aes.Create())
            {
                aes.BlockSize = 128;
                aes.Mode = CipherMode.CBC;
                aes.Key = encryptedToken.Key;
                aes.IV = encryptedToken.IV;

                using (var decryptor = aes.CreateDecryptor())
                {
                    var decryptedBytes = decryptor.TransformFinalBlock(encryptedToken.Bytes, 0, encryptedToken.Bytes.Length);
                    return Encoding.UTF8.GetString(decryptedBytes);
                }
            }
        }
    }

    public class SnowflakeCredentialManagerInMemoryImpl : ISnowflakeCredentialManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerInMemoryImpl>();

        private static readonly Dictionary<string, EncryptedToken> s_credentials = new Dictionary<string, EncryptedToken>();

        public static readonly SnowflakeCredentialManagerInMemoryImpl Instance = new SnowflakeCredentialManagerInMemoryImpl();

        public string GetCredentials(string key)
        {
            EncryptedToken token;
            s_credentials.TryGetValue(key, out token);

            s_logger.Debug($"Getting credentials from memory for key: {key}");
            if (token.Bytes == null)
            {
                s_logger.Info("Unable to get credentials for the specified key");
                return "";
            }
            else
            {
                return SnowflakeCredentialEncryption.DecryptToken(s_credentials[key]);
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
            s_credentials[key] = SnowflakeCredentialEncryption.EncryptToken(token);
        }
    }
}
