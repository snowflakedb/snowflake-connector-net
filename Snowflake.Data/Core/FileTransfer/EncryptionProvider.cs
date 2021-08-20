using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Crypto.IO;

using System.IO;
using System;
using Snowflake.Data.Log;
using Org.BouncyCastle.Crypto.Paddings;
using Org.BouncyCastle.Crypto;

namespace Snowflake.Data.Core.FileTransfer
{
    /// <summary>
    /// Helper class to encrypt and decrypt data
    /// </summary>
    class EncryptionProvider
    {
        /// <summary>
        /// The file and key ciphers algorithm.
        /// </summary>
        private static readonly string ALGO = "AES";

        /// <summary>
        /// The file cipher mode.
        /// </summary>
        private static readonly string FILE_MODE = "CBC";

        /// <summary>
        /// The key cipher mode.
        /// </summary>
        private static readonly string KEY_MODE = "ECB";

        /// <summary>
        /// The file and key ciphers padding.
        /// NOTE : Bouncy castle maps AES/PKCS5 to AES/PKCS7.
        /// PKCS5 = PKCS7 with block size 8
        /// </summary>
        private static readonly string PADDING = "PKCS7PADDING";

        /// <summary>
        /// The file cipher.
        /// </summary>
        private static readonly string FILE_CIPHER = ALGO + "/" + FILE_MODE  + "/" + PADDING;

        /// <summary>
        /// The key cipher.
        /// </summary>
        private static readonly string KEY_CIPHER = ALGO + "/" + KEY_MODE + "/" + PADDING;

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<EncryptionProvider>();

        /// <summary>
        /// Creates an encrypting stream to encrypt the content of 'inputStream' and returns the 
        /// encrypted encryption key and iv used for encrypting the data.
        /// </summary>
        /// <param name="queryStageMasterKey">The master key to encrypt the encryption key.</param>
        /// <param name="inputStream">The raw input data to encrypt.</param>
        /// <param name="iv"> The IV used for encrypting the data.</param>
        /// <param name="encryptedFileKey">The encrypted encryption key.</param>
        /// <returns></returns>
        public CipherStream EncryptingStream(
            string queryStageMasterKey, 
            Stream inputStream,
            out byte[] iv,
            out byte[] encryptedFileKey,
            out IBufferedCipher cipher)
        {
            byte[] decodedMasterKey = Convert.FromBase64String(queryStageMasterKey);
            int masterKeySize = decodedMasterKey.Length * 8;
            Logger.Debug($"Master key size : {masterKeySize}");

            // Create and encode the file key with the QRMK
            encryptedFileKey = generateAndEncryptFileKey(queryStageMasterKey);

            // Create the encrypting stream
            cipher = CipherUtilities.GetCipher(FILE_CIPHER);
            var random = new SecureRandom();
            // Create the IV
            iv = random.GenerateSeed(cipher.GetBlockSize());
            var fileKeyParameters = new ParametersWithIV(new KeyParameter(encryptedFileKey), iv);
            cipher.Init(true, fileKeyParameters);
            return new CipherStream(inputStream, null, cipher);

        }

        /// <summary>
        /// Creates a decrypting stream to decrypt the content 'inputStream' of and returns 
        /// </summary>
        /// <param name="iv">The IV used for encrypting the data.</param>
        /// <param name="encryptedKey">The encrypted encryption key.</param>
        /// <param name="queryStageMasterKey">The master key used to encrypt the encryption key.</param>
        /// <param name="inputStream">The raw input data to decrypt.</param>
        /// <returns></returns>
        public CipherStream DecryptingStream(
            string iv,
            string encryptedKey,
            string queryStageMasterKey,
            Stream inputStream)
        {
            byte[] decodeFileKey = Convert.FromBase64String(encryptedKey);
            byte[] decodedIv = Convert.FromBase64String(iv);

            // Decrypt the file key with the QRMK
            byte[] fileKey = decryptFileKey(queryStageMasterKey, decodeFileKey);

            // Decrypt the data
            var cipher = CipherUtilities.GetCipher(FILE_CIPHER);
            var fileKeyParameters = new ParametersWithIV(new KeyParameter(fileKey), decodedIv);
            cipher.Init(false, fileKeyParameters);

            return new CipherStream(inputStream, cipher, null);
        }

        /// <summary>
        /// Generate a new file encryption key and encrypt this key using the QRMK.
        /// </summary>
        /// <param name="queryStageMasterKey">The QRMK.</param>
        /// <returns>The encrypted encryption key.</returns>
        private byte[] generateAndEncryptFileKey(string queryStageMasterKey)
        {

            byte[] decodedMasterKey = Convert.FromBase64String(queryStageMasterKey);

            // Create the file key
            var random = new SecureRandom();
            var unencryptedFileKey = random.GenerateSeed(decodedMasterKey.Length);

            var keyCipher = CipherUtilities.GetCipher(KEY_CIPHER);
            var keyParameters = ParameterUtilities.CreateKeyParameter(ALGO, decodedMasterKey);
            keyCipher.Init(true, keyParameters);
            return keyCipher.DoFinal(unencryptedFileKey);
        }

        /// <summary>
        /// Decrypt the file encryption key using the QRMK.
        /// </summary>
        /// <param name="queryStageMasterKey">The QRMK.</param>
        /// <param name="encryptedKey">The encrypted encryption key.</param>
        /// <returns>The decrypted encryption key.</returns>
        private byte[] decryptFileKey(string queryStageMasterKey, byte[] encryptedKey)
        {

            byte[] decodedMasterKey = Convert.FromBase64String(queryStageMasterKey);

            var keyCipher = CipherUtilities.GetCipher(KEY_CIPHER);
            var keyParameters = ParameterUtilities.CreateKeyParameter(ALGO, decodedMasterKey);
            keyCipher.Init(false, keyParameters);
            return keyCipher.DoFinal(encryptedKey);
        }
    }
}
