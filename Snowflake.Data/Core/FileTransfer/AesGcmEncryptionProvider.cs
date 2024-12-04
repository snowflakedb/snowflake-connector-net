#if NETSTANDARD2_1
using System;
using System.IO;
using System.Security.Cryptography;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class AesGcmEncryptionProvider
    {
        private const int AesTagBits = 128;
        internal const int TagSizeInBytes = AesTagBits / 8;
        private const int InitVectorSizeInBytes = 12;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<AesGcmEncryptionProvider>();
        private static readonly Random s_random = new Random();

        public static Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] contentAad,
            byte[] keyAad
        )
        {
            var fileBytes = File.ReadAllBytes(inFile);
            return Encrypt(encryptionMaterial, encryptionMetadata,  fileBytes, contentAad, keyAad);
        }

        public static Stream DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            var fileBytes = File.ReadAllBytes(inFile);
            return Decrypt(fileBytes, encryptionMaterial, encryptionMetadata);
        }

        public static Stream Encrypt(
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata, // this is output parameter
            byte[] inputBytes,
            byte[] contentAad,
            byte[] keyAad)
        {
            byte[] decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
            int masterKeySize = decodedMasterKey.Length;
            s_logger.Debug($"Master key size : {masterKeySize}");

            var contentIV = new byte[InitVectorSizeInBytes];
            var keyIV = new byte[InitVectorSizeInBytes];
            var fileKeyBytes = new byte[masterKeySize]; // we choose a random fileKey to encrypt it with qsmk key with GCM
            s_random.NextBytes(contentIV);
            s_random.NextBytes(keyIV);
            s_random.NextBytes(fileKeyBytes);

            var encryptedKey = EncryptBytes(fileKeyBytes, decodedMasterKey, keyIV, keyAad);
            var result = EncryptBytes(inputBytes, fileKeyBytes, contentIV, contentAad);

            MaterialDescriptor matDesc = new MaterialDescriptor
            {
                smkId = encryptionMaterial.smkId.ToString(),
                queryId = encryptionMaterial.queryId,
                keySize = (masterKeySize * 8).ToString()
            };

            encryptionMetadata.key = Convert.ToBase64String(encryptedKey);
            encryptionMetadata.iv = Convert.ToBase64String(contentIV);
            encryptionMetadata.keyIV = Convert.ToBase64String(keyIV);
            encryptionMetadata.keyAad = keyAad == null ? null : Convert.ToBase64String(keyAad);
            encryptionMetadata.aad = contentAad == null ? null : Convert.ToBase64String(contentAad);
            encryptionMetadata.matDesc = Newtonsoft.Json.JsonConvert.SerializeObject(matDesc);

            return new MemoryStream(result);
        }

        public static Stream Decrypt(
            byte[] inputBytes,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            var decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
            var keyBytes = Convert.FromBase64String(encryptionMetadata.key);
            var keyIVBytes = Convert.FromBase64String(encryptionMetadata.keyIV);
            var keyAad = encryptionMetadata.keyAad == null ? null : Convert.FromBase64String(encryptionMetadata.keyAad);
            var ivBytes = Convert.FromBase64String(encryptionMetadata.iv);
            var contentAad = encryptionMetadata.aad == null ? null : Convert.FromBase64String(encryptionMetadata.aad);
            var decryptedFileKey = DecryptBytes(keyBytes, decodedMasterKey, keyIVBytes, keyAad);
            var decryptedContent = DecryptBytes(inputBytes, decryptedFileKey, ivBytes, contentAad);
            return new MemoryStream(decryptedContent);
        }

        private static byte[] EncryptBytes(byte[] inputBytes, byte[] keyBytes, byte[] iv, byte[] aad)
        {
            using (var aesGcm = new AesGcm(keyBytes))
            {
                var encodedBytes = new byte[inputBytes.Length + TagSizeInBytes];
                var cipherBytes = encodedBytes.AsSpan(0, inputBytes.Length);
                var authenticationTag = encodedBytes.AsSpan(inputBytes.Length, TagSizeInBytes);
                aesGcm.Encrypt(iv, inputBytes, cipherBytes, authenticationTag, aad);
                return encodedBytes;
            }
        }

        private static byte[] DecryptBytes(byte[] inputBytes, byte[] keyBytes, byte[] iv, byte[] aad)
        {
            using (var aesGcm = new AesGcm(keyBytes))
            {
                var cipherBytes = inputBytes.AsSpan(0, inputBytes.Length - TagSizeInBytes);
                var decryptedBytes = new byte[cipherBytes.Length];
                var authenticationTag = inputBytes.AsSpan(inputBytes.Length - TagSizeInBytes, TagSizeInBytes);
                aesGcm.Decrypt(iv, cipherBytes, authenticationTag, decryptedBytes, aad);
                return decryptedBytes;
            }
        }
    }
}
#endif
