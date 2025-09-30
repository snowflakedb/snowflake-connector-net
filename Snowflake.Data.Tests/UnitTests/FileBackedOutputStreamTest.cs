using System.IO;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests
{
    public class FileBackedOutputStreamTest
    {
        private const string ShortText = "short text";
        private const int MaxBytesInMemory = 50;
        private static readonly string s_longText = RandomJsonGenerator.GenerateRandomJsonString(5);

        [Test]
        public void TestThatSwitchesFromMemoryToFileOnGivenThresholdAndAllowsToReadAll()
        {
            // expect
            Assert.IsTrue(ShortText.Length < MaxBytesInMemory);
            Assert.IsTrue(s_longText.Length > MaxBytesInMemory);

            // arrange
            var stream = new FileBackedOutputStream(MaxBytesInMemory, Path.GetTempPath());

            // assert
            Assert.IsFalse(stream.IsUsingFileOutputStream());

            // arrange
            var bytesSmallEnoughToResideInMemory = Encoding.ASCII.GetBytes(ShortText);

            // act
            stream.Write(bytesSmallEnoughToResideInMemory, 0, bytesSmallEnoughToResideInMemory.Length);

            // assert
            Assert.IsFalse(stream.IsUsingFileOutputStream());

            // act
            ToByteStream(s_longText).CopyTo(stream);

            // assert
            Assert.IsTrue(stream.IsUsingFileOutputStream());

            // act
            var memoryStream = new MemoryStream();
            stream.Position = 0;
            stream.CopyTo(memoryStream);

            // assert
            var allTextFromStream = Encoding.ASCII.GetString(memoryStream.ToArray());
            Assert.AreEqual(ShortText + s_longText, allTextFromStream);
        }

        [Test]
        public void TestThatAfterDisposeNoTemporaryFileExists()
        {
            // expect
            Assert.IsTrue(s_longText.Length > MaxBytesInMemory);

            // arrange
            var stream = new FileBackedOutputStream(MaxBytesInMemory, Path.GetTempPath());
            ToByteStream(s_longText).CopyTo(stream);

            // assert
            Assert.IsTrue(stream.IsUsingFileOutputStream());
            var fileName = stream.GetFileName();
            Assert.IsTrue(File.Exists(fileName));

            // act
            stream.Dispose();

            // assert
            Assert.IsFalse(File.Exists(fileName));
        }

        private MemoryStream ToByteStream(string value) => new MemoryStream(Encoding.ASCII.GetBytes(value));
    }
}
