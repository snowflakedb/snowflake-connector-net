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
        static public byte[] EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            using (FileStream fileStream = File.OpenRead(inFile))
            {
                MemoryStream ms = new MemoryStream();
                fileStream.CopyTo(ms);
                return EncryptStream(ms, encryptionMaterial, encryptionMetadata);
            }
        }

        /// <summary>
        /// Encrypt data and write to the outStream.
        /// </summary>
        /// <param name="memoryStream">The data to write onto the stream.</param>
        /// <param name="encryptionMaterial">Contains the query stage master key, query id, and smk id.</param>
        /// <param name="encryptionMetadata">Store the encryption metadata into</param>
        /// <returns>The encrypted bytes of the file to upload.</returns>
        static public byte[] EncryptStream(
            MemoryStream memoryStream,
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
                memoryStream,
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
        /// <param name="memoryStream">The path of the file to write onto the stream.</param>
        /// <param name="key">The encryption key.</param>
        /// <param name="iv">The encryption IV or null if it needs to be generated.</param>
        /// <returns>The encrypted bytes.</returns>
        static private byte[] CreateEncryptedBytes(
            MemoryStream memoryStream,
            byte[] key,
            byte[] iv)
        {
            Aes aes = Aes.Create();            
            aes.Key = key;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.IV = iv;
            memoryStream.Position = 0;

            MemoryStream targetStream = new MemoryStream();
            CryptoStream cryptoStream = new CryptoStream(targetStream, aes.CreateEncryptor(), CryptoStreamMode.Write);
            byte[] buffer = new byte[1024000];
            int bytesRead;
            while ((bytesRead = memoryStream.Read(buffer, 0, buffer.Length)) > 0)
            {
                cryptoStream.Write(buffer, 0, bytesRead);
            }
            cryptoStream.FlushFinalBlock();

            return targetStream.ToArray();
        }

        /// <summary>
        /// Decrypt data and write to the outStream.
        /// </summary>
        /// <param name="inFile">The data to write onto the stream.</param>
        /// <param name="encryptionMaterial">Contains the query stage master key, query id, and smk id.</param>
        /// <param name="encryptionMetadata">Store the encryption metadata into</param>
        /// <returns>The encrypted bytes of the file to upload.</returns>
        static public string DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            // Get key and iv from metadata
            string keyBase64 = encryptionMetadata.key;
            string ivBase64 = encryptionMetadata.iv;

            // Get decoded key from base64 encoded value
            byte[] decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);

            // Get key bytes and iv bytes from base64 encoded value
            byte[] keyBytes = Convert.FromBase64String(keyBase64);
            byte[] ivBytes = Convert.FromBase64String(ivBase64);

            // Create temp file
            string tempFileName = Path.Combine(Path.GetTempPath(), Path.GetFileName(inFile));

            // Create decipher with file key, iv bytes, and AES CBC
            byte[] decryptedFileKey = decryptFileKey(decodedMasterKey, keyBytes);

            // Create key decipher with decoded key and AES ECB
            byte[] decryptedBytes = CreateDecryptedBytes(
                inFile,
                decryptedFileKey,
                ivBytes);

            File.WriteAllBytes(tempFileName, decryptedBytes);

            return tempFileName;
        }

        /// <summary>
        /// Decrypt the newly generated file key using the master key.
        /// </summary>
        /// <param name="masterKey">The key to use for encryption.</param>
        /// <param name="unencryptedFileKey">The file key to encrypt.</param>
        /// <returns>The encrypted key.</returns>
        static private byte[] decryptFileKey(byte[] masterKey, byte[] unencryptedFileKey)
        {
            Aes aes = Aes.Create();
            aes.Key = masterKey;
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.PKCS7;

            MemoryStream cipherStream = new MemoryStream();
            CryptoStream cryptoStream = new CryptoStream(cipherStream, aes.CreateDecryptor(), CryptoStreamMode.Write);
            cryptoStream.Write(unencryptedFileKey, 0, unencryptedFileKey.Length);
            cryptoStream.FlushFinalBlock();

            return cipherStream.ToArray();
        }

        /// <summary>
        /// Creates a new byte array containing the decrypted data.
        /// </summary>
        /// <param name="inFile">The path of the file to write onto the stream.</param>
        /// <param name="key">The encryption key.</param>
        /// <param name="iv">The encryption IV or null if it needs to be generated.</param>
        /// <returns>The encrypted bytes.</returns>
        static private byte[] CreateDecryptedBytes(
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
            CryptoStream cryptoStream = new CryptoStream(targetStream, aes.CreateDecryptor(), CryptoStreamMode.Write);

            using(Stream inStream = File.OpenRead(inFile))
            {
                inStream.CopyTo(cryptoStream);
            }

            return targetStream.ToArray();
        }
    }
}
