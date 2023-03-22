using System.IO;

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
            return EncryptionStream.Create(stream, EncryptionStream.CryptMode.Encrypt, encryptionMaterial, encryptionMetadata, leaveOpen);
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
            // Create temp file
            string tempFileName = Path.Combine(Path.GetTempPath(), Path.GetFileName(inFile));

            using (var readStream = EncryptionStream.Create(File.OpenRead(inFile), EncryptionStream.CryptMode.Decrypt, encryptionMaterial, encryptionMetadata, false))
            {
                using (var writeStream = File.Create(tempFileName))
                {
                    readStream.CopyTo(writeStream);
                }
            }

            return tempFileName;
        }
    }
}
