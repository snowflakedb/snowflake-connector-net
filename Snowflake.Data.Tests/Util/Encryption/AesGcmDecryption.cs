#if NETSTANDARD2_1
using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests.Util
{
    internal class AesGcmDecryption : IDecryptionProvider
    {
        public static readonly AesGcmDecryption Instance = new AesGcmDecryption();

        public Stream DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            return AesGcmEncryptionProvider.DecryptFile(
                inFile,
                encryptionMaterial,
                encryptionMetadata);
        }

        public Stream Decrypt(
            byte[] inputBytes,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            return AesGcmEncryptionProvider.Decrypt(
                inputBytes,
                encryptionMaterial,
                encryptionMetadata);
        }

        public string ExpectedExceptionMessage() => "The computed authentication tag did not match the input authentication tag.";
    }
}
#endif
