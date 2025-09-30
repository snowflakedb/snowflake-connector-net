using Snowflake.Data.Tests.Util;
using NUnit.Framework;
using System;
using System.IO;
using System.Text;

namespace Snowflake.Data.Tests.UnitTests
{

    [TestFixture]
    class ConcatenatedStreamTest
    {
        Stream _concatStream;

        [SetUp]
        public void BeforeTest()
        {
            string data = "12345678";
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Stream stream = new MemoryStream(bytes);

            Stream openBracket = new MemoryStream(Encoding.UTF8.GetBytes(data));
            Stream closeBracket = new MemoryStream(Encoding.UTF8.GetBytes(data));

            _concatStream = new ConcatenatedStream(new Stream[3] { openBracket, stream, closeBracket });
        }

        [Test]
        public void TestCanRead()
        {
            Assert.IsTrue(_concatStream.CanRead);
        }

        [Test]
        public void TestCanSeek()
        {
            Assert.IsFalse(_concatStream.CanSeek);
        }

        [Test]
        public void TestCanWrite()
        {
            Assert.IsFalse(_concatStream.CanWrite);
        }

        [Test]
        public void TestFlush()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Flush());
        }

        [Test]
        public void TestLength()
        {
            Assert.Throws<NotImplementedException>(() => _ = _concatStream.Length);
        }

        [Test]
        public void TestGetPosition()
        {
            Assert.Throws<NotImplementedException>(() => _ = _concatStream.Position);
        }

        [Test]
        public void TestSetPosition()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Position = 1);
        }

        [Test]
        public void TestSeek()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Seek(1, new SeekOrigin()));
        }

        [Test]
        public void TestSetLength()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.SetLength(1));
        }

        [Test]
        public void TestWrite()
        {
            Assert.Throws<NotImplementedException>(() => _concatStream.Write(null, 0, 0));
        }

        [Test]
        public void TestReadZeroByte()
        {
            byte[] buffer = new byte[0];
            Assert.AreEqual(0, _concatStream.Read(buffer, 0, 0)); // Read 0 byte
        }

        [Test]
        public void TestReadBytes()
        {
            byte[] buffer = new byte[3];
            Assert.AreEqual(1, _concatStream.Read(buffer, 0, 1)); // Read 1 byte
            Assert.AreEqual(2, _concatStream.Read(buffer, 0, 2)); // Read 2 bytes
            Assert.AreEqual(3, _concatStream.Read(buffer, 0, 3)); // Read 3 bytes
        }

        [Test]
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
                Assert.IsTrue(ex is ArgumentException);
            }
        }
    }
}
