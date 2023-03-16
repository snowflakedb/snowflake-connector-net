using System;
using System.IO;
using System.Security.Cryptography;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;

    [TestFixture]
    class EncryptionStreamTest
    {
        [Test]
        public void CanWriteAndReadEncryptedStream()
        {
            var randomFileNameCrypt = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            var random = RandomNumberGenerator.Create();
            var rawData = new byte[100000];
            random.GetBytes(rawData);

            var pgMaterial = new PutGetEncryptionMaterial();
            var metaData = new SFEncryptionMetadata();

            using (var aes = Aes.Create())
            {
                // create a random master key
                var keyBytes = new byte[aes.KeySize/8];
                random.GetBytes(keyBytes);
                pgMaterial.queryStageMasterKey = Convert.ToBase64String(keyBytes);
            }
            
            try
            {
                var readStream = new MemoryStream(rawData, false);
                // first, write the data to the crypt file
                using (var encryptStream = new EncryptionStream(File.OpenWrite(randomFileNameCrypt), EncryptionStream.CryptMode.Encrypt, pgMaterial, metaData, false))
                {
                    readStream.CopyTo(encryptStream);
                }

                var newStream = new MemoryStream();
                // next read the data from the encrypted file into the newStream
                using (var decryptStream = new EncryptionStream(File.OpenRead(randomFileNameCrypt), EncryptionStream.CryptMode.Decrypt, pgMaterial, metaData, false))
                {
                    decryptStream.CopyTo(newStream);
                }

                var newBytes = newStream.ToArray();

                // make sure the bytes are the same
                Assert.AreEqual(rawData, newBytes);
            }
            finally
            {
                // cleanup the temporary file
                File.Delete(randomFileNameCrypt);
            }
        }

    }
}
