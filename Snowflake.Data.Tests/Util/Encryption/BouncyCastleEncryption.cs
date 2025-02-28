using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.UnitTests;

namespace Snowflake.Data.Tests.Util
{
    internal class BouncyCastleEncryption: IEncryptionProvider
    {
        public static readonly BouncyCastleEncryption Instance = new BouncyCastleEncryption();

        public Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] contentAad,
            byte[] keyAad
        )
        {
            return GcmEncryptionProvider.EncryptFile(
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
            return GcmEncryptionProvider.Encrypt(
                encryptionMaterial,
                encryptionMetadata,
                GcmEncryptionProviderTest.s_fileTransferConfiguration,
                new MemoryStream(inputBytes),
                contentAad,
                keyAad);
        }
    }

}
