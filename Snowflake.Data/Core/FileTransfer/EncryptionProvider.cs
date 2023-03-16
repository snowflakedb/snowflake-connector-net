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
        static public Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            return EncryptStream(File.OpenRead(inFile), encryptionMaterial, encryptionMetadata, false);
        }

        /// <summary>
        /// Encrypt data and write to the outStream.
        /// </summary>
        /// <param name="stream">The data to write onto the stream.</param>
        /// <param name="encryptionMaterial">Contains the query stage master key, query id, and smk id.</param>
        /// <param name="encryptionMetadata">Store the encryption metadata into</param>
        /// <param name="leaveOpen">Pass true to avoid closing the <paramref name="stream"/>></param>
        /// <returns>The encrypted bytes of the file to upload.</returns>
        static public Stream EncryptStream(
            Stream stream,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            bool leaveOpen)
        {
            return new EncryptionStream(stream, encryptionMaterial, encryptionMetadata, leaveOpen);
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
            cryptoStream.FlushFinalBlock();

            return targetStream.ToArray();
        }
    }
}
