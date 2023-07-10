﻿/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using System;
    using System.IO;
    using System.Text;

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
        [Ignore("ConcatenatedStreamTest")]
        public void ConcatenatedStreamTestDone()
        {
            // Do nothing;
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
            try
            {
                _concatStream.Flush();
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
        }

        [Test]
        public void TestLength()
        {
            try
            {
                _ = _concatStream.Length;
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
        }

        [Test]
        public void TestGetPosition()
        {
            try
            {
                _ = _concatStream.Position;
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
        }

        [Test]
        public void TestSetPosition()
        {
            try
            {
                _concatStream.Position = 1;
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
        }

        [Test]
        public void TestSeek()
        {
            try
            {
                _concatStream.Seek(1, new SeekOrigin());
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
        }

        [Test]
        public void TestSetLength()
        {
            try
            {
                _concatStream.SetLength(1);
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
        }

        [Test]
        public void TestWrite()
        {
            try
            {
                _concatStream.Write(null, 0, 0);
                Assert.Fail("A NotImplementedException should've been thrown");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is NotImplementedException);
            }
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
                _concatStream.Read(buffer, 0, 4);
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
