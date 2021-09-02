using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using Snowflake.Data.Core.FileTransfer;
    using System.IO;

    [TestFixture]
    class SFEncryptionProviderTest
    {
        private static string masterKey = "bBWR4SlSefWBtTG9CRngJQ==";

        [Test]
        public void TestEncryptionProvider()
        {
            EncryptionProvider encryptionProvider = new EncryptionProvider();

            byte[] dataToEncode = Encoding.UTF8.GetBytes("Some data to encrypt. And to décrypt :)");
            byte[] encryptedData;
            string ivBase64;
            string keyBase64;

            using (var encryptedStream = new MemoryStream())
            {
                using (var cryptoStream = encryptionProvider.CreateEncryptionStream(
                           encryptedStream,
                           masterKey,
                           out ivBase64,
                           out keyBase64))
                {

                    // Encode data into encryptedStream
                    cryptoStream.Write(dataToEncode, 0, dataToEncode.Length);
                    cryptoStream.FlushFinalBlock(); // Don't forget to flush the final block!


                    encryptedStream.Seek(0, SeekOrigin.Begin);
                    encryptedData = encryptedStream.ToArray();

                    string encodedData = Convert.ToBase64String(encryptedData, Base64FormattingOptions.None);

                    Console.WriteLine(encodedData);
                }
            }


            using (var decryptedStream = new MemoryStream())
            {
                using (var cryptoStream = encryptionProvider.CreateDecryptionStream(
                        decryptedStream,
                        masterKey,
                        ivBase64,
                        keyBase64))
                {

                    // Decode data
                    cryptoStream.Write(encryptedData, 0, encryptedData.Length);
                    cryptoStream.FlushFinalBlock(); // Don't forget to flush the final block!

                    // Go back to the beginning of the stream to read it
                    decryptedStream.Seek(0, SeekOrigin.Begin);

                    byte[] decryptedData = decryptedStream.ToArray();
                    var result = Encoding.UTF8.GetString(decryptedData);
                    Assert.AreEqual(Encoding.UTF8.GetString(dataToEncode), result);

                    Console.WriteLine(result);
                }
            }
        }
    }
}
