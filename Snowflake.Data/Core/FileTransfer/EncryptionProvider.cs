using System.IO;
using System;
using Snowflake.Data.Log;
using System.Security.Cryptography;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.FileTransfer
{
    /// <summary>
    /// The encryptor/decryptor for PUT/GET files.
    /// Handles encryption and decryption using AES CBC (for files) and ECB (for keys).
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
        /// <param name="transferConfiguration">Contains parameters used during encryption process</param>
        /// <returns>The encrypted bytes of the file to upload.</returns>
        public static StreamPair EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            FileTransferConfiguration transferConfiguration)
        {
            using (var fileStream = File.OpenRead(inFile))
            {
                return EncryptStream(fileStream, encryptionMaterial, encryptionMetadata, transferConfiguration);
            }
        }

        /// <summary>
        /// Encrypt data and write to the outStream.
        /// </summary>
        /// <param name="inputStream">The data to write onto the stream.</param>
        /// <param name="encryptionMaterial">Contains the query stage master key, query id, and smk id.</param>
        /// <param name="encryptionMetadata">Store the encryption metadata into</param>
        /// <param name="transferConfiguration">Contains parameters used during encryption process</param>
        /// <returns>The encrypted bytes of the file to upload.</returns>
        public static StreamPair EncryptStream(
            Stream inputStream,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            FileTransferConfiguration transferConfiguration)
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

            var encryptedBytesStream = CreateEncryptedBytesStream(
                inputStream,
                keyData,
                ivData,
                transferConfiguration);

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

            return encryptedBytesStream;
        }

        /// <summary>
        /// Encrypt the newly generated file key using the master key.
        /// </summary>
        /// <param name="masterKey">The key to use for encryption.</param>
        /// <param name="unencryptedFileKey">The file key to encrypt.</param>
        /// <returns>The encrypted key.</returns>
        private static byte[] encryptFileKey(byte[] masterKey, byte[] unencryptedFileKey)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = masterKey;
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.PKCS7;

                MemoryStream cipherStream = new MemoryStream();
                using (var encryptor = aes.CreateEncryptor())
                using (CryptoStream cryptoStream = new CryptoStream(cipherStream, encryptor, CryptoStreamMode.Write))
                {
                    cryptoStream.Write(unencryptedFileKey, 0, unencryptedFileKey.Length);
                    cryptoStream.FlushFinalBlock();
                    return cipherStream.ToArray();
                }
            }
        }

        /// <summary>
        /// Creates a new byte array containing the encrypted/decrypted data.
        /// </summary>
        /// <param name="inputStream">The input stream to encrypt</param>
        /// <param name="key">The encryption key.</param>
        /// <param name="iv">The encryption IV or null if it needs to be generated.</param>
        /// <param name="transferConfiguration">Contains parameters used during encryption process</param>
        /// <returns>The encrypted bytes.</returns>
        private static StreamPair CreateEncryptedBytesStream(
            Stream inputStream,
            byte[] key,
            byte[] iv,
            FileTransferConfiguration transferConfiguration)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = key;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                aes.IV = iv;
                inputStream.Position = 0;

                var targetStream = new FileBackedOutputStream(transferConfiguration.MaxBytesInMemory, transferConfiguration.TempDir);
                using (var encryptor = aes.CreateEncryptor())
                {
                    CryptoStream cryptoStream = new CryptoStream(targetStream, encryptor, CryptoStreamMode.Write);
                    byte[] buffer = new byte[transferConfiguration.MaxBytesInMemory];
                    int bytesRead;
                    while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        cryptoStream.Write(buffer, 0, bytesRead);
                    }
                    cryptoStream.FlushFinalBlock();

                    return new StreamPair
                    {
                        MainStream = targetStream,
                        HelperStream = cryptoStream // cryptoStream cannot be closed here because it would close target stream as well
                    };
                }
            }
        }

        /// <summary>
        /// Decrypt data and write to the outStream.
        /// </summary>
        /// <param name="inFile">The data to write onto the stream.</param>
        /// <param name="encryptionMaterial">Contains the query stage master key, query id, and smk id.</param>
        /// <param name="encryptionMetadata">Store the encryption metadata into</param>
        /// <param name="transferConfiguration">Contains parameters used during decryption process</param>
        /// <returns>The name of the file containing decrypted file bytes</returns>
        public static string DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            FileTransferConfiguration transferConfiguration)
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
            using (var decryptedBytesStreamPair = CreateDecryptedBytesStream(
                       inFile,
                       decryptedFileKey,
                       ivBytes,
                       transferConfiguration))
            {
                using (var decryptedFileStream = FileOperations.Instance.CreateTempFile(tempFileName))
                {
                    var decryptedBytesStream = decryptedBytesStreamPair.MainStream;
                    decryptedBytesStream.Position = 0;
                    decryptedBytesStream.CopyTo(decryptedFileStream);
                }
            }
            return tempFileName;
        }

        /// <summary>
        /// Decrypt the newly generated file key using the master key.
        /// </summary>
        /// <param name="masterKey">The key to use for encryption.</param>
        /// <param name="unencryptedFileKey">The file key to encrypt.</param>
        /// <returns>The encrypted key.</returns>
        private static byte[] decryptFileKey(byte[] masterKey, byte[] unencryptedFileKey)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = masterKey;
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.PKCS7;

                MemoryStream cipherStream = new MemoryStream();
                using (var decryptor = aes.CreateDecryptor())
                using (CryptoStream cryptoStream = new CryptoStream(cipherStream, decryptor, CryptoStreamMode.Write))
                {
                    cryptoStream.Write(unencryptedFileKey, 0, unencryptedFileKey.Length);
                    cryptoStream.FlushFinalBlock();

                    return cipherStream.ToArray();
                }
            }
        }

        /// <summary>
        /// Creates a new byte array containing the decrypted data.
        /// </summary>
        /// <param name="inFile">The path of the file to write onto the stream.</param>
        /// <param name="key">The encryption key.</param>
        /// <param name="iv">The encryption IV or null if it needs to be generated.</param>
        /// <returns>The decrypted bytes stream</returns>
        private static StreamPair CreateDecryptedBytesStream(
            string inFile,
            byte[] key,
            byte[] iv,
            FileTransferConfiguration transferConfiguration)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = key;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                aes.IV = iv;

                var targetStream = new FileBackedOutputStream(transferConfiguration.MaxBytesInMemory, transferConfiguration.TempDir);
                using (var decryptor = aes.CreateDecryptor())
                {
                    CryptoStream cryptoStream = new CryptoStream(targetStream, decryptor, CryptoStreamMode.Write);
                    using (Stream inStream = File.OpenRead(inFile))
                    {
                        inStream.CopyTo(cryptoStream);
                    }
                    cryptoStream.FlushFinalBlock();

                    return new StreamPair
                    {
                        MainStream = targetStream,
                        HelperStream = cryptoStream // cryptoStream cannot be closed here because it would close target stream as well
                    };
                }
            }
        }
    }
}
