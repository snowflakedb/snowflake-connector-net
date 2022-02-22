using System.IO;
using System;
using Snowflake.Data.Log;
using System.Security.Cryptography;

namespace Snowflake.Data.Core.FileTransfer
{
    /// <summary>
    /// The encryption materials. 
    /// </summary>
    internal class MaterialDescriptor
    {
        public string smkId { get; set; }

        public string queryId { get; set; }

        public string keySize { get; set; }
    }

    /// <summary>
    /// The encryptor/decryptor for PUT/GET files. 
    /// </summary>
    class EncryptionProvider
    {
        // The default block size for AES
        private const int AES_BLOCK_SIZE = 128;
        private const int blockSize = AES_BLOCK_SIZE / 8;  // in bytes

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<EncryptionProvider>();

        /// <summary>
        /// Encrypt data and write to the outStream.
        /// </summary>
        /// <param name="inFile">The data to write onto the stream.</param>
        /// <param name="encryptionMaterial">Contains the query stage master key, query id, and smk id.</param>
        /// <param name="encryptionMetadata">Store the encryption metadata into</param>
        /// <returns>The encrypted bytes of the file to upload.</returns>
        static public byte[] CreateEncryptedBytes(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            byte[] decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
            int masterKeySize = decodedMasterKey.Length;
            Logger.Debug($"Master key size : {masterKeySize}");

            // Generate file key
            byte[] ivData = new byte[blockSize];
            byte[] keyData = new byte[blockSize];

            var random = new Random();
            random.NextBytes(ivData);
            random.NextBytes(keyData);

            // Byte[] to encrypt data into
            byte[] encryptedBytes = CreateEncryptedBytes(
                inFile,
                keyData,
                ivData);

            // Encrypt file key
            byte[] encryptedFileKey = encryptFileKey(decodedMasterKey, keyData);
            
            // Store encryption metadata information
            MaterialDescriptor matDesc = new MaterialDescriptor
            {
                smkId = encryptionMaterial.smkId.ToString(),
                queryId = encryptionMaterial.queryId,
                keySize = (masterKeySize * 8).ToString()
            };

            string ivBase64 = Convert.ToBase64String(ivData);
            string keyBase64 = Convert.ToBase64String(encryptedFileKey);

            encryptionMetadata.iv = ivBase64;
            encryptionMetadata.key = keyBase64;
            encryptionMetadata.matDesc = Newtonsoft.Json.JsonConvert.SerializeObject(matDesc).ToString();

            return encryptedBytes;
        }

        /// <summary>
        /// Encrypt the newly generated file key using the master key.
        /// </summary>
        /// <param name="masterKey">The key to use for encryption.</param>
        /// <param name="unencryptedFileKey">The file key to encrypt.</param>
        /// <returns>The encrypted key.</returns>
        static private byte[] encryptFileKey(byte[] masterKey, byte[] unencryptedFileKey)
        {
            Aes aes = Aes.Create();            
            aes.Key = masterKey;
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.PKCS7;

            MemoryStream cipherStream = new MemoryStream();
            CryptoStream cryptoStream = new CryptoStream(cipherStream, aes.CreateEncryptor(), CryptoStreamMode.Write);            
            cryptoStream.Write(unencryptedFileKey, 0, unencryptedFileKey.Length);
            cryptoStream.FlushFinalBlock();

            return cipherStream.ToArray();
        }

        /// <summary>
        /// Creates a new byte array containing the encrypted/decrypted data.
        /// </summary>
        /// <param name="inFile">The path of the file to write onto the stream.</param>
        /// <param name="key">The encryption key.</param>
        /// <param name="iv">The encryption IV or null if it needs to be generated.</param>
        /// <returns>The encrypted bytes.</returns>
        static private byte[] CreateEncryptedBytes(
            string inFile,
            byte[] key,
            byte[] iv)
        {
            Aes aes = Aes.Create();            
            aes.Key = key;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.IV = iv;

            MemoryStream targetStream = new MemoryStream();
            CryptoStream cryptoStream = new CryptoStream(targetStream, aes.CreateEncryptor(), CryptoStreamMode.Write);
            
            byte[] inFileBytes = File.ReadAllBytes(inFile);
            cryptoStream.Write(inFileBytes, 0, inFileBytes.Length);
            cryptoStream.FlushFinalBlock();

            return targetStream.ToArray();                   
        }        
    }
}
