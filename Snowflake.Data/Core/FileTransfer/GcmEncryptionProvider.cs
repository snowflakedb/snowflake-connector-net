using System;
using System.IO;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.IO;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.FileTransfer
{
    internal class GcmEncryptionProvider
    {
        private const int TagSizeInBits = 128;
        internal const int TagSizeInBytes = TagSizeInBits / 8;
        private const int InitVectorSizeInBytes = 12;
        private const string AesGcmNoPaddingCipher = "AES/GCM/NoPadding";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<GcmEncryptionProvider>();

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

            return result;
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
            return DecryptContent(inputStream, decryptedFileKey, ivBytes, contentAad, fileTransferConfiguration);
        }

        private static byte[] EncryptKey(byte[] fileKeyBytes, byte[] qsmk, byte[] keyIV, byte[] keyAad)
        {
            var keyCipher = BuildAesGcmNoPaddingCipher(true, qsmk, keyIV, keyAad);
            var cipherKeyData = new byte[keyCipher.GetOutputSize(fileKeyBytes.Length)];
            var processLength = keyCipher.ProcessBytes(fileKeyBytes, 0, fileKeyBytes.Length, cipherKeyData, 0);
            keyCipher.DoFinal(cipherKeyData, processLength);
            return cipherKeyData;
        }

        private static Stream EncryptContent(Stream inputStream, byte[] fileKeyBytes, byte[] contentIV, byte[] contentAad,
            FileTransferConfiguration transferConfiguration)
        {
            var contentCipher = BuildAesGcmNoPaddingCipher(true, fileKeyBytes, contentIV, contentAad);
            var targetStream = new FileBackedOutputStream(transferConfiguration.MaxBytesInMemory, transferConfiguration.TempDir);
            try
            {
                var cipherStream = new CipherStream(targetStream, null, contentCipher);
                byte[] buffer = new byte[transferConfiguration.MaxBytesInMemory];
                int bytesRead;
                while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    cipherStream.Write(buffer, 0, bytesRead);
                }

                cipherStream.Flush(); // we cannot close or dispose cipherStream because: 1) it would do additional DoFinal resulting in an exception 2) closing cipherStream would close target stream
                var mac = contentCipher.DoFinal(); // getting authentication tag for the whole content
                targetStream.Write(mac, 0, mac.Length);
                return targetStream;
            }
            catch (Exception)
            {
                targetStream.Dispose();
                throw;
            }
        }

        private static byte[] DecryptKey(byte[] fileKey, byte[] qsmk, byte[] keyIV, byte[] keyAad)
        {
            var keyCipher = BuildAesGcmNoPaddingCipher(false, qsmk, keyIV, keyAad);
            var decryptedKeyData = new byte[keyCipher.GetOutputSize(fileKey.Length)];
            var processLength = keyCipher.ProcessBytes(fileKey, 0, fileKey.Length, decryptedKeyData, 0);
            keyCipher.DoFinal(decryptedKeyData, processLength);
            return decryptedKeyData;
        }

        private static Stream DecryptContent(Stream inputStream, byte[] fileKeyBytes, byte[] contentIV, byte[] contentAad,
            FileTransferConfiguration transferConfiguration)
        {
            var contentCipher = BuildAesGcmNoPaddingCipher(false, fileKeyBytes, contentIV, contentAad);
            var targetStream = new FileBackedOutputStream(transferConfiguration.MaxBytesInMemory, transferConfiguration.TempDir);
            try
            {
                var cipherStream = new CipherStream(targetStream, null, contentCipher);
                byte[] buffer = new byte[transferConfiguration.MaxBytesInMemory];
                int bytesRead;
                while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    cipherStream.Write(buffer, 0, bytesRead);
                }
                cipherStream.Flush(); // we cannot close or dispose cipherStream because closing cipherStream would close target stream
                var lastBytes = contentCipher.DoFinal();
                if (lastBytes != null && lastBytes.Length > 0)
                {
                    targetStream.Write(lastBytes, 0, lastBytes.Length);
                }
                return targetStream;
            }
            catch (Exception)
            {
                targetStream.Dispose();
                throw;
            }
        }

        private static IBufferedCipher BuildAesGcmNoPaddingCipher(bool forEncryption, byte[] keyBytes, byte[] initialisationVector, byte[] aadData)
        {
            var cipher = CipherUtilities.GetCipher(AesGcmNoPaddingCipher);
            KeyParameter keyParameter = new KeyParameter(keyBytes);
            var keyParameterAead = aadData == null
                ? new AeadParameters(keyParameter, TagSizeInBits, initialisationVector)
                : new AeadParameters(keyParameter, TagSizeInBits, initialisationVector, aadData);
            cipher.Init(forEncryption, keyParameterAead);
            return cipher;
        }
    }
}
