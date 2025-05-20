using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.BouncyCastle.Crypto;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class GcmEncryptionProviderTest
    {
        private const string PlainText = "there is no rose without thorns";
        private static readonly byte[] s_plainTextBytes = Encoding.UTF8.GetBytes(PlainText);
        private static readonly byte[] s_qsmkBytes = TestDataGenarator.NextBytes(GcmEncryptionProvider.TagSizeInBytes);
        private static readonly string s_qsmk = Convert.ToBase64String(s_qsmkBytes);
        private static readonly string s_queryId = Guid.NewGuid().ToString();
        private const long SmkId = 1234L;
        private const string KeyAad = "key additional information";
        private static readonly byte[] s_keyAadBytes = Encoding.UTF8.GetBytes(KeyAad);
        private static readonly string s_keyAadBase64 = Convert.ToBase64String(s_keyAadBytes);
        private const string ContentAad = "content additional information";
        private static readonly byte[] s_contentAadBytes = Encoding.UTF8.GetBytes(ContentAad);
        private static readonly string s_contentAadBase64 = Convert.ToBase64String(s_contentAadBytes);
        private const string InvalidAad = "invalid additional information";
        private static readonly byte[] s_invalidAadBytes = Encoding.UTF8.GetBytes(InvalidAad);
        private static readonly string s_invalidAadBase64 = Convert.ToBase64String(s_invalidAadBytes);
        private static readonly string s_emptyAad = string.Empty;
        private static readonly byte[] s_emptyAadBytes = Encoding.UTF8.GetBytes(s_emptyAad);
        private static readonly string s_emptyAadBase64 = Convert.ToBase64String(s_emptyAadBytes);
        private static readonly PutGetEncryptionMaterial s_encryptionMaterial = new PutGetEncryptionMaterial
        {
            queryStageMasterKey = s_qsmk,
            queryId = s_queryId,
            smkId = SmkId
        };
        private static readonly FileTransferConfiguration s_fileTransferConfiguration = new FileTransferConfiguration
        {
            TempDir = Path.GetTempPath(),
            MaxBytesInMemory = FileTransferConfiguration.DefaultMaxBytesInMemory
        };

        [Test]
        public void TestEncryptAndDecryptWithoutAad()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();

            // act
            using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                       s_encryptionMaterial,
                       encryptionMetadata, // this is output parameter
                       s_fileTransferConfiguration,
                       new MemoryStream(s_plainTextBytes),
                       null,
                       null))
            {
                var encryptedContent = ExtractContentBytes(encryptedStream);

                // assert
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                Assert.IsNull(encryptionMetadata.keyAad);
                Assert.IsNull(encryptionMetadata.aad);

                // act
                using (var decryptedStream = GcmEncryptionProvider.Decrypt(new MemoryStream(encryptedContent), s_encryptionMaterial, encryptionMetadata, s_fileTransferConfiguration))
                {
                    // assert
                    var decryptedText = ExtractContent(decryptedStream);
                    CollectionAssert.AreEqual(s_plainTextBytes, decryptedText);
                }
            }
        }

        [Test]
        public void TestEncryptAndDecryptWithEmptyAad()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();

            // act
            using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                       s_encryptionMaterial,
                       encryptionMetadata, // this is output parameter
                       s_fileTransferConfiguration,
                       new MemoryStream(s_plainTextBytes),
                       s_emptyAadBytes,
                       s_emptyAadBytes))
            {
                var encryptedContent = ExtractContentBytes(encryptedStream);

                // assert
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                Assert.AreEqual(s_emptyAadBase64, encryptionMetadata.keyAad);
                Assert.AreEqual(s_emptyAadBase64, encryptionMetadata.aad);

                // act
                using (var decryptedStream = GcmEncryptionProvider.Decrypt(new MemoryStream(encryptedContent), s_encryptionMaterial, encryptionMetadata, s_fileTransferConfiguration))
                {
                    // assert
                    var decryptedText = ExtractContent(decryptedStream);
                    CollectionAssert.AreEqual(s_plainTextBytes, decryptedText);
                }
            }
        }

        [Test]
        public void TestEncryptAndDecryptWithAad()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();

            // act
            using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                       s_encryptionMaterial,
                       encryptionMetadata, // this is output parameter
                       s_fileTransferConfiguration,
                       new MemoryStream(s_plainTextBytes),
                       s_contentAadBytes,
                       s_keyAadBytes))
            {
                var encryptedContent = ExtractContentBytes(encryptedStream);

                // assert
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                CollectionAssert.AreEqual(s_keyAadBase64, encryptionMetadata.keyAad);
                CollectionAssert.AreEqual(s_contentAadBase64, encryptionMetadata.aad);

                // act
                using (var decryptedStream = GcmEncryptionProvider.Decrypt(new MemoryStream(encryptedContent), s_encryptionMaterial, encryptionMetadata, s_fileTransferConfiguration))
                {
                    // assert
                    var decryptedText = ExtractContent(decryptedStream);
                    CollectionAssert.AreEqual(s_plainTextBytes, decryptedText);
                }
            }
        }

        [Test]
        public void TestFailDecryptWithInvalidKeyAad()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                       s_encryptionMaterial,
                       encryptionMetadata, // this is output parameter
                       s_fileTransferConfiguration,
                       new MemoryStream(s_plainTextBytes),
                       null,
                       s_keyAadBytes))
            {
                var encryptedContent = ExtractContentBytes(encryptedStream);
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                CollectionAssert.AreEqual(s_keyAadBase64, encryptionMetadata.keyAad);
                Assert.IsNull(encryptionMetadata.aad);
                encryptionMetadata.keyAad = s_invalidAadBase64;

                // act
                var thrown = Assert.Throws<InvalidCipherTextException>(() =>
                    GcmEncryptionProvider.Decrypt(new MemoryStream(encryptedContent), s_encryptionMaterial, encryptionMetadata, s_fileTransferConfiguration));

                // assert
                Assert.AreEqual("mac check in GCM failed", thrown.Message);
            }
        }

        [Test]
        public void TestFailDecryptWithInvalidContentAad()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                       s_encryptionMaterial,
                       encryptionMetadata, // this is output parameter
                       s_fileTransferConfiguration,
                       new MemoryStream(s_plainTextBytes),
                       s_contentAadBytes,
                       null))
            {
                var encryptedContent = ExtractContentBytes(encryptedStream);
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                Assert.IsNull(encryptionMetadata.keyAad);
                CollectionAssert.AreEqual(s_contentAadBase64, encryptionMetadata.aad);
                encryptionMetadata.aad = s_invalidAadBase64;

                // act
                var thrown = Assert.Throws<InvalidCipherTextException>(() =>
                    GcmEncryptionProvider.Decrypt(new MemoryStream(encryptedContent), s_encryptionMaterial, encryptionMetadata, s_fileTransferConfiguration));

                // assert
                Assert.AreEqual("mac check in GCM failed", thrown.Message);
            }
        }

        [Test]
        public void TestFailDecryptWhenMissingAad()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                       s_encryptionMaterial,
                       encryptionMetadata, // this is output parameter
                       s_fileTransferConfiguration,
                       new MemoryStream(s_plainTextBytes),
                       s_contentAadBytes,
                       s_keyAadBytes))
            {
                var encryptedContent = ExtractContentBytes(encryptedStream);
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                CollectionAssert.AreEqual(s_keyAadBase64, encryptionMetadata.keyAad);
                CollectionAssert.AreEqual(s_contentAadBase64, encryptionMetadata.aad);
                encryptionMetadata.keyAad = null;
                encryptionMetadata.aad = null;

                // act
                var thrown = Assert.Throws<InvalidCipherTextException>(() =>
                    GcmEncryptionProvider.Decrypt(new MemoryStream(encryptedContent), s_encryptionMaterial, encryptionMetadata, s_fileTransferConfiguration));

                // assert
                Assert.AreEqual("mac check in GCM failed", thrown.Message);
            }
        }

        [Test]
        public void TestEncryptAndDecryptFile()
        {
            // arrange
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            var plainTextFilePath = Path.Combine(Path.GetTempPath(), "plaintext.txt");
            var encryptedFilePath = Path.Combine(Path.GetTempPath(), "encrypted.txt");
            try
            {
                CreateFile(plainTextFilePath, PlainText);

                // act
                using (var encryptedStream = GcmEncryptionProvider.EncryptFile(plainTextFilePath, s_encryptionMaterial, encryptionMetadata,
                           s_fileTransferConfiguration, s_contentAadBytes, s_keyAadBytes))
                {
                    CreateFile(encryptedFilePath, encryptedStream);
                }

                // assert
                Assert.NotNull(encryptionMetadata.key);
                Assert.NotNull(encryptionMetadata.iv);
                Assert.NotNull(encryptionMetadata.matDesc);
                CollectionAssert.AreEqual(s_keyAadBase64, encryptionMetadata.keyAad);
                CollectionAssert.AreEqual(s_contentAadBase64, encryptionMetadata.aad);

                // act
                string result;
                using (var decryptedStream = GcmEncryptionProvider.DecryptFile(encryptedFilePath, s_encryptionMaterial, encryptionMetadata,
                           s_fileTransferConfiguration))
                {
                    decryptedStream.Position = 0;
                    var resultBytes = new byte[decryptedStream.Length];
                    var bytesRead = decryptedStream.Read(resultBytes, 0, resultBytes.Length);
                    Assert.AreEqual(decryptedStream.Length, bytesRead);
                    result = Encoding.UTF8.GetString(resultBytes);
                }

                // assert
                CollectionAssert.AreEqual(PlainText, result);
            }
            finally
            {
                File.Delete(plainTextFilePath);
                File.Delete(encryptedFilePath);
            }
        }

        private static void CreateFile(string filePath, string content)
        {
            using (var writer = File.CreateText(filePath))
            {
                writer.Write(content);
            }
        }

        private static void CreateFile(string filePath, Stream content)
        {
            using (var writer = File.Create(filePath))
            {
                var buffer = new byte[1024];
                int bytesRead;
                content.Position = 0;
                while ((bytesRead = content.Read(buffer, 0, 1024)) > 0)
                {
                    writer.Write(buffer, 0, bytesRead);
                }
            }
        }

        private string ExtractContent(Stream stream) =>
            Encoding.UTF8.GetString(ExtractContentBytes(stream));

        private byte[] ExtractContentBytes(Stream stream)
        {
            var memoryStream = new MemoryStream();
            stream.Position = 0;
            stream.CopyTo(memoryStream);
            return memoryStream.ToArray();
        }
    }
}
