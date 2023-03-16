using Snowflake.Data.Log;
using System;
using System.IO;
using System.Security.Cryptography;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class EncryptionStream : Stream
    {
        private readonly bool leaveOpen;

        // The default block size for AES
        private const int AES_BLOCK_SIZE = 128;
        private const int blockSize = AES_BLOCK_SIZE / 8;  // in bytes

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<EncryptionProvider>();

        private static readonly RandomNumberGenerator Randomizer = RandomNumberGenerator.Create();
        /// <summary>
        /// the stream to read/write the encrypted data
        /// </summary>
        private readonly Stream targetStream;
        
        public enum CryptMode
        {
            Encrypt,
            Decrypt
        }

        /// <summary>
        /// Creates an encryption stream
        /// </summary>
        /// <param name="stream">The stream to read or write to containing encrypted data</param>
        /// <param name="mode">The crypto mode to use</param>
        /// <param name="encryptionMaterial"></param>
        /// <param name="encryptionMetadata"></param>
        /// <param name="leaveOpen"></param>
        public EncryptionStream(
            Stream stream,
            CryptMode mode,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            bool leaveOpen)
        {
            this.leaveOpen = leaveOpen;
            this.Mode = mode;

            if (mode == CryptMode.Encrypt)
            {
                byte[] decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
                int masterKeySize = decodedMasterKey.Length;
                Logger.Debug($"Master key size : {masterKeySize}");

                // Generate file key

                byte[] ivData = new byte[blockSize];
                byte[] keyData = new byte[blockSize];
                Randomizer.GetBytes(ivData);
                Randomizer.GetBytes(keyData);

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

                using (Aes aes = Aes.Create())
                {
                    aes.Key = keyData;
                    aes.Mode = CipherMode.CBC;
                    aes.Padding = PaddingMode.PKCS7;
                    aes.IV = ivData;

                    this.targetStream = new CryptoStream(stream, aes.CreateEncryptor(), CryptoStreamMode.Write, leaveOpen: this.leaveOpen);
                }
            }
            else
            {
                // Get key and iv from metadata
                string keyBase64 = encryptionMetadata.key;
                string ivBase64 = encryptionMetadata.iv;

                // Get decoded key from base64 encoded value
                byte[] decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);

                // Get key bytes and iv bytes from base64 encoded value
                byte[] keyBytes = Convert.FromBase64String(keyBase64);
                byte[] ivBytes = Convert.FromBase64String(ivBase64);

                // Create decipher with file key, iv bytes, and AES CBC
                byte[] decryptedFileKey = decryptFileKey(decodedMasterKey, keyBytes);

                // Create key decipher with decoded key and AES ECB
                using (Aes aes = Aes.Create())
                {
                    aes.Key = decryptedFileKey;
                    aes.Mode = CipherMode.CBC;
                    aes.Padding = PaddingMode.PKCS7;
                    aes.IV = ivBytes;

                    this.targetStream = new CryptoStream(stream, aes.CreateDecryptor(), CryptoStreamMode.Write, leaveOpen: leaveOpen);
                }
            }
        }

        public CryptMode Mode { get; }

        public override void Flush()
        {
            this.targetStream.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.targetStream.Dispose();
            }

            base.Dispose(disposing);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.targetStream.Write(buffer, offset, count);
        }

        public override bool CanRead => Mode == CryptMode.Decrypt;
        public override bool CanSeek => false;
        public override bool CanWrite => Mode == CryptMode.Encrypt;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();

            set => throw new NotSupportedException();
        }

        /// <summary>
        /// Encrypt the newly generated file key using the master key.
        /// </summary>
        /// <param name="masterKey">The key to use for encryption.</param>
        /// <param name="unencryptedFileKey">The file key to encrypt.</param>
        /// <returns>The encrypted key.</returns>
        static private byte[] encryptFileKey(byte[] masterKey, byte[] unencryptedFileKey)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = masterKey;
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.PKCS7;

                MemoryStream cipherStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(cipherStream, aes.CreateEncryptor(), CryptoStreamMode.Write);
                cryptoStream.Write(unencryptedFileKey, 0, unencryptedFileKey.Length);
                cryptoStream.FlushFinalBlock();

                return cipherStream.ToArray();
            }
        }


        /// <summary>
        /// Decrypt the newly generated file key using the master key.
        /// </summary>
        /// <param name="masterKey">The key to use for encryption.</param>
        /// <param name="unencryptedFileKey">The file key to encrypt.</param>
        /// <returns>The encrypted key.</returns>
        static private byte[] decryptFileKey(byte[] masterKey, byte[] unencryptedFileKey)
        {
            using (Aes aes = Aes.Create())
            {
                aes.Key = masterKey;
                aes.Mode = CipherMode.ECB;
                aes.Padding = PaddingMode.PKCS7;

                MemoryStream cipherStream = new MemoryStream();
                CryptoStream cryptoStream = new CryptoStream(cipherStream, aes.CreateDecryptor(), CryptoStreamMode.Write);
                cryptoStream.Write(unencryptedFileKey, 0, unencryptedFileKey.Length);
                cryptoStream.FlushFinalBlock();

                return cipherStream.ToArray();
            }
        }
    }
}
