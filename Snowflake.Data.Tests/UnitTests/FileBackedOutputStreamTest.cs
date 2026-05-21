using System.IO;
using System.Text;
using Xunit;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests
{
    public class FileBackedOutputStreamTest
    {
        private const string ShortText = "short text";
        private const int MaxBytesInMemory = 50;
        private static readonly string s_longText = RandomJsonGenerator.GenerateRandomJsonString(5);

        [SFFact]
        public void TestThatSwitchesFromMemoryToFileOnGivenThresholdAndAllowsToReadAll()
        {
            // expect
            Assert.True(ShortText.Length < MaxBytesInMemory);
            Assert.True(s_longText.Length > MaxBytesInMemory);

            // arrange
            var stream = new FileBackedOutputStream(MaxBytesInMemory, Path.GetTempPath());

            // assert
            Assert.False(stream.IsUsingFileOutputStream());

            // arrange
            var bytesSmallEnoughToResideInMemory = Encoding.ASCII.GetBytes(ShortText);

            // act
            stream.Write(bytesSmallEnoughToResideInMemory, 0, bytesSmallEnoughToResideInMemory.Length);

            // assert
            Assert.False(stream.IsUsingFileOutputStream());

            // act
            ToByteStream(s_longText).CopyTo(stream);

            // assert
            Assert.True(stream.IsUsingFileOutputStream());

            // act
            var memoryStream = new MemoryStream();
            stream.Position = 0;
            stream.CopyTo(memoryStream);

            // assert
            var allTextFromStream = Encoding.ASCII.GetString(memoryStream.ToArray());
            Assert.Equal(ShortText + s_longText, allTextFromStream);
        }

        [SFFact]
        public void TestThatAfterDisposeNoTemporaryFileExists()
        {
            // expect
            Assert.True(s_longText.Length > MaxBytesInMemory);

            // arrange
            var stream = new FileBackedOutputStream(MaxBytesInMemory, Path.GetTempPath());
            ToByteStream(s_longText).CopyTo(stream);

            // assert
            Assert.True(stream.IsUsingFileOutputStream());
            var fileName = stream.GetFileName();
            Assert.True(File.Exists(fileName));

            // act
            stream.Dispose();

            // assert
            Assert.False(File.Exists(fileName));
        }

        private MemoryStream ToByteStream(string value) => new MemoryStream(Encoding.ASCII.GetBytes(value));
    }
}
