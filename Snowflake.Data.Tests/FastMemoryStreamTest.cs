/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using System.Linq;

    [TestFixture]
    class FastMemoryStreamTest
    {
        FastMemoryStream fastMemoryStream;

        [SetUp]
        public void BeforeTest()
        {
            fastMemoryStream = new FastMemoryStream();
        }

        [Test]
        [Ignore("FastMemoryStreamTest")]
        public void FastMemoryStreamTestDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestDefaultValues()
        {
            // FastMemoryStream defaults to size of 0 and buffer of size 256
            Assert.AreEqual(0, fastMemoryStream.Length);
            Assert.AreEqual(FastMemoryStream.DEFAULT_BUFFER_SIZE, fastMemoryStream.GetBuffer().Length);
        }

        [Test]
        public void TestWriteByte()
        {
            byte val = 32;
            int bufferLength = fastMemoryStream.GetBuffer().Length;

            byte[] byteArr = Enumerable.Repeat(val, bufferLength).ToArray();

            for (int i = 0; i < bufferLength; i++)
            {
                fastMemoryStream.WriteByte(val);
            }

            Assert.AreEqual(byteArr, fastMemoryStream.GetBuffer());
        }

        [Test]
        public void TestClear()
        {
            int bufferLength = fastMemoryStream.GetBuffer().Length;
            for (int i = 0; i < bufferLength; i++)
            {
                fastMemoryStream.WriteByte(1);
            }

            // Size should be equal to the buffer length
            Assert.AreEqual(bufferLength, fastMemoryStream.Length);
            // Clear() resets the size
            fastMemoryStream.Clear();
            // Size should be back to 0
            Assert.AreEqual(0, fastMemoryStream.Length);
        }

        [Test]
        public void TestBufferIsIncreasedWhenSizeIsLargerThanDefault()
        {
            byte val = 32;
            // Multiply default buffer length by 2 to make fastMemoryStream increase the buffer
            int bufferLength = fastMemoryStream.GetBuffer().Length * 2;

            byte[] byteArr = Enumerable.Repeat(val, bufferLength).ToArray();

            for (int i = 0; i < bufferLength; i++)
            {
                fastMemoryStream.WriteByte(val);
            }

            Assert.AreEqual(byteArr, fastMemoryStream.GetBuffer());
        }
    }
}
