using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.UnitTests;

namespace Snowflake.Data.Tests.Util
{
    internal class BouncyCastleFipsEncryption: IEncryptionProvider
    {
        public static readonly BouncyCastleFipsEncryption Instance = new BouncyCastleFipsEncryption();

        public Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] contentAad,
            byte[] keyAad
        )
        {
            return GcmFipsEncryptionProvider.EncryptFile(
                inFile,
                encryptionMaterial,
                encryptionMetadata,
                GcmEncryptionProviderTest.s_fileTransferConfiguration,
                contentAad,
                keyAad);
        }

        public Stream Encrypt(
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] inputBytes,
            byte[] contentAad,
            byte[] keyAad)
        {
            return GcmFipsEncryptionProvider.Encrypt(
                encryptionMaterial,
                encryptionMetadata,
                GcmEncryptionProviderTest.s_fileTransferConfiguration,
                new MemoryStream(inputBytes),
                contentAad,
                keyAad);
        }
    }

}
