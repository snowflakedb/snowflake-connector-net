using Snowflake.Data.Tests.Util;
using Xunit;
using System;
using System.IO;
using System.Text;

namespace Snowflake.Data.Tests.UnitTests
{
    sealed class ConcatenatedStreamTest
    {
        Stream _concatStream;

        public ConcatenatedStreamTest()
        {
            string data = "12345678";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);

            Stream openBracket = new MemoryStream(Encoding.UTF8.GetBytes(data));
            Stream closeBracket = new MemoryStream(Encoding.UTF8.GetBytes(data));

            _concatStream = new ConcatenatedStream(new Stream[3] { openBracket, stream, closeBracket });
        }

        [SFFact]
        public void TestCanRead()
        {
            Assert.True(_concatStream.CanRead);
        }

        [SFFact]
        public void TestCanSeek()
        {
            Assert.False(_concatStream.CanSeek);
        }

        [SFFact]
        public void TestCanWrite()
        {
            Assert.False(_concatStream.CanWrite);
        }

        [SFFact]
        public void TestFlush()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Flush());
        }

        [SFFact]
        public void TestLength()
        {
            Assert.Throws<NotImplementedException>(() => _ = _concatStream.Length);
        }

        [SFFact]
        public void TestGetPosition()
        {
            Assert.Throws<NotImplementedException>(() => _ = _concatStream.Position);
        }

        [SFFact]
        public void TestSetPosition()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Position = 1);
        }

        [SFFact]
        public void TestSeek()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Seek(1, new SeekOrigin()));
        }

        [SFFact]
        public void TestSetLength()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.SetLength(1));
        }

        [SFFact]
        public void TestWrite()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Write(null, 0, 0));
        }

        [SFFact]
        public void TestReadZeroByte()
        {
            byte[] buffer = new byte[0];
            Assert.Equal(0, _concatStream.Read(buffer, 0, 0)); // Read 0 byte
        }

        [SFFact]
        public void TestReadBytes()
        {
            byte[] buffer = new byte[3];
            Assert.Equal(1, _concatStream.Read(buffer, 0, 1)); // Read 1 byte
            Assert.Equal(2, _concatStream.Read(buffer, 0, 2)); // Read 2 bytes
            Assert.Equal(3, _concatStream.Read(buffer, 0, 3)); // Read 3 bytes
        }

        [SFFact]
        public void TestReadMoreBytesThanBufferSize()
        {
            byte[] buffer = new byte[3];

            try
            {
                // An ArgumentException is thrown when 4 bytes is read from a buffer of size 3
                var readBytes = _concatStream.Read(buffer, 0, 4);
                Assert.Fail("An ArgumentException should've been thrown");
            }
            catch (Exception ex)
            {
                // NET 4.7.1 throws an ArgumentException
                // NET 4.7.2 throws an ArgumentOutOfRangeException
                Assert.True(ex is ArgumentException);
            }
        }
    }
}
