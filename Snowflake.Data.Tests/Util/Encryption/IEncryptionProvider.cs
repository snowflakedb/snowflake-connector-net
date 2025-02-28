using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests.Util
{
    internal interface IEncryptionProvider
    {
        Stream EncryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] contentAad,
            byte[] keyAad);

        Stream Encrypt(
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata,
            byte[] inputBytes,
            byte[] contentAad,
            byte[] keyAad);
    }
}
