using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.UnitTests;

namespace Snowflake.Data.Tests.Util
{
    internal class BouncyCastleDecryption : IDecryptionProvider
    {
        public static readonly BouncyCastleDecryption Instance = new BouncyCastleDecryption();

        public Stream DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            return GcmEncryptionProvider.DecryptFile(
                inFile,
                encryptionMaterial,
                encryptionMetadata,
                GcmEncryptionProviderTest.s_fileTransferConfiguration);
        }

        public Stream Decrypt(
            byte[] inputBytes,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            return GcmEncryptionProvider.Decrypt(
                new MemoryStream(inputBytes),
                encryptionMaterial,
                encryptionMetadata,
                GcmEncryptionProviderTest.s_fileTransferConfiguration);
        }

        public string ExpectedExceptionMessage() =>
            "mac check in GCM failed";
    }
}
