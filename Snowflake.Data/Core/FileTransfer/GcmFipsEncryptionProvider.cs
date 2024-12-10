extern alias BouncyCastleFips;
using System;
using System.IO;
using BouncyCastleFips::Org.BouncyCastle.Security;
using BouncyCastleFips::Org.BouncyCastle.Crypto.Fips;
using BouncyCastleFips::Org.BouncyCastle.Crypto;
using BouncyCastleFips::Org.BouncyCastle.Utilities.IO;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class GcmFipsEncryptionProvider
    {
        private const int TagSizeInBits = 128;
        internal const int TagSizeInBytes = TagSizeInBits / 8;
        private const int InitVectorSizeInBytes = 12;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<GcmFipsEncryptionProvider>();

        private static readonly SecureRandom s_random = SecureRandom.GetInstance("SHA1PRNG");

        public static Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            FileTransferConfiguration transferConfiguration,
            byte[] contentAad,
            byte[] keyAad
        )
        {
            using (var fileStream = File.OpenRead(inFile))
            {
                return Encrypt(encryptionMaterial, encryptionMetadata, transferConfiguration, fileStream, contentAad, keyAad);
            }
        }

        public static Stream DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            FileTransferConfiguration transferConfiguration)
        {
            using (var fileStream = File.OpenRead(inFile))
            {
                return Decrypt(fileStream, encryptionMaterial, encryptionMetadata, transferConfiguration);
            }
        }

        public static Stream Encrypt(
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata, // this is output parameter
            FileTransferConfiguration fileTransferConfiguration,
            Stream inputStream,
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

            var encryptedKey = EncryptKey(fileKeyBytes, decodedMasterKey, keyIV, keyAad);
            var result = EncryptContent(inputStream, fileKeyBytes, contentIV, contentAad, fileTransferConfiguration);

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
            Stream inputStream,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            FileTransferConfiguration fileTransferConfiguration)
        {
            var decodedMasterKey = Convert.FromBase64String(encryptionMaterial.queryStageMasterKey);
            var keyBytes = Convert.FromBase64String(encryptionMetadata.key);
            var keyIVBytes = Convert.FromBase64String(encryptionMetadata.keyIV);
            var keyAad = encryptionMetadata.keyAad == null ? null : Convert.FromBase64String(encryptionMetadata.keyAad);
            var ivBytes = Convert.FromBase64String(encryptionMetadata.iv);
            var contentAad = encryptionMetadata.aad == null ? null : Convert.FromBase64String(encryptionMetadata.aad);
            var decryptedFileKey = DecryptKey(keyBytes, decodedMasterKey, keyIVBytes, keyAad);
            var decryptedContent = DecryptContent(inputStream, decryptedFileKey, ivBytes, contentAad, fileTransferConfiguration);
            return new MemoryStream(decryptedContent);
        }

        private static byte[] EncryptKey(byte[] fileKeyBytes, byte[] qsmk, byte[] keyIV, byte[] keyAad)
        {
            var outputStream = new MemoryOutputStream();
            var keyCipher = BuildEncryptor(true, outputStream, qsmk, keyIV, keyAad);
            using (Stream encryptionStream = keyCipher.Stream)
            {
                encryptionStream.Write(fileKeyBytes, 0, fileKeyBytes.Length);
                encryptionStream.Flush();
            }
            var outputBytes = outputStream.ToArray();
            return outputBytes;
        }

        private static byte[] DecryptKey(byte[] fileKeyBytes, byte[] qsmk, byte[] keyIV, byte[] keyAad)
        {
            var outputStream = new MemoryOutputStream();
            var keyCipher = BuildEncryptor(false, outputStream, qsmk, keyIV, keyAad);
            using (Stream encryptionStream = keyCipher.Stream)
            {
                encryptionStream.Write(fileKeyBytes, 0, fileKeyBytes.Length);
                encryptionStream.Flush();
            }
            var outputBytes = outputStream.ToArray();
            return outputBytes;
        }

        private static byte[] EncryptContent(Stream inputStream, byte[] fileKeyBytes, byte[] contentIV, byte[] contentAad,
            FileTransferConfiguration transferConfiguration)
        {
            var targetStream = new MemoryOutputStream();
            var contentCipher = BuildEncryptor(true, targetStream, fileKeyBytes, contentIV, contentAad);
            using (var encryptionStream = contentCipher.Stream)
            {
                byte[] buffer = new byte[transferConfiguration.MaxBytesInMemory];
                int bytesRead;
                while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    encryptionStream.Write(buffer, 0, bytesRead);
                }
                encryptionStream.Flush();
            }
            var outputBytes = targetStream.ToArray();
            return outputBytes;
        }

        private static byte[] DecryptContent(Stream inputStream, byte[] fileKeyBytes, byte[] contentIV, byte[] contentAad,
            FileTransferConfiguration transferConfiguration)
        {
            var targetStream = new MemoryOutputStream();
            var contentCipher = BuildEncryptor(false, targetStream, fileKeyBytes, contentIV, contentAad);
            using (var encryptionStream = contentCipher.Stream)
            {
                byte[] buffer = new byte[transferConfiguration.MaxBytesInMemory];
                int bytesRead;
                while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    encryptionStream.Write(buffer, 0, bytesRead);
                }
                encryptionStream.Flush();
            }
            var outputBytes = targetStream.ToArray();
            return outputBytes;
        }

        private static IAeadCipher BuildEncryptor(bool forEncryption, Stream outputStream, byte[] keyBytes, byte[] initialisationVector, byte[] aadData)
        {
            var serviceProvider = CryptoServicesRegistrar.CreateService(new FipsAes.Key(keyBytes));
            var algorithmDetails = FipsAes.Gcm.WithIV(initialisationVector).WithMacSize(TagSizeInBits);
            var encryptorBuilder = forEncryption
                ? serviceProvider.CreateAeadEncryptorBuilder(algorithmDetails)
                : serviceProvider.CreateAeadDecryptorBuilder(algorithmDetails);
            var encryptor = (IAeadCipher) encryptorBuilder.BuildCipher(outputStream);
            if (aadData != null)
            {
                encryptor.AadStream.Write(aadData, 0, aadData.Length);
            }
            return encryptor;
        }
    }
}
