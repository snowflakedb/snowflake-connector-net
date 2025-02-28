using System.IO;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests.Util
{
    internal interface IDecryptionProvider
    {
        Stream DecryptFile(
            string inFile,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata);

        Stream Decrypt(
            byte[] inputBytes,
            PutGetEncryptionMaterial encryptionMaterial,
            SFEncryptionMetadata encryptionMetadata);

        string ExpectedExceptionMessage();
    }
}
