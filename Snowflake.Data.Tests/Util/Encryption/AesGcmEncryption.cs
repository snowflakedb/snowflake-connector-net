#if NETSTANDARD2_1
using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests.Util
{
    internal class AesGcmEncryption : IEncryptionProvider
    {
        public static readonly AesGcmEncryption Instance = new AesGcmEncryption();

        public Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] contentAad,
            byte[] keyAad
        )
        {
            return AesGcmEncryptionProvider.EncryptFile(
                inFile,
                encryptionMaterial,
                encryptionMetadata,
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
            return AesGcmEncryptionProvider.Encrypt(
                encryptionMaterial,
                encryptionMetadata,
                inputBytes,
                contentAad,
                keyAad);
        }
    }
}
#endif
