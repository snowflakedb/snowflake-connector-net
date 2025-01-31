using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.UnitTests;

namespace Snowflake.Data.Tests.Util
{
    internal class BouncyCastleFipsDecryption : IDecryptionProvider
    {
        public static readonly BouncyCastleFipsDecryption Instance = new BouncyCastleFipsDecryption();

        public Stream DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata)
        {
            return GcmFipsEncryptionProvider.DecryptFile(
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
            return GcmFipsEncryptionProvider.Decrypt(
                new MemoryStream(inputBytes),
                encryptionMaterial,
                encryptionMetadata,
                GcmEncryptionProviderTest.s_fileTransferConfiguration);
        }

        public string ExpectedExceptionMessage() =>
            "mac check in GCM failed";
    }
}
