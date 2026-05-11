namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Core;
    using System.Linq;
    class FastMemoryStreamTest
    {
        FastMemoryStream _fastMemoryStream;
        public void BeforeTest()
        {
            _fastMemoryStream = new FastMemoryStream();
        }

        [Fact]
        public void TestDefaultValues()
        {
            // FastMemoryStream defaults to size of 0 and buffer of size 256
            Assert.Equal(0, _fastMemoryStream.Length);
            Assert.Equal(FastMemoryStream.DEFAULT_BUFFER_SIZE, _fastMemoryStream.GetBuffer().Length);
        }

        [Fact]
        public void TestWriteByte()
        {
            byte val = 32;
            int bufferLength = _fastMemoryStream.GetBuffer().Length;

            byte[] byteArr = Enumerable.Repeat(val, bufferLength).ToArray();

            for (int i = 0; i < bufferLength; i++)
            {
                _fastMemoryStream.WriteByte(val);
            }

            Assert.Equal(byteArr, _fastMemoryStream.GetBuffer());
        }

        [Fact]
        public void TestClear()
        {
            int bufferLength = _fastMemoryStream.GetBuffer().Length;
            for (int i = 0; i < bufferLength; i++)
            {
                _fastMemoryStream.WriteByte(1);
            }

            // Size should be equal to the buffer length
            Assert.Equal(bufferLength, _fastMemoryStream.Length);
            // Clear() resets the size
            _fastMemoryStream.Clear();
            // Size should be back to 0
            Assert.Equal(0, _fastMemoryStream.Length);
        }

        [Fact]
        public void TestBufferIsIncreasedWhenSizeIsLargerThanDefault()
        {
            byte val = 32;
            // Multiply default buffer length by 2 to make fastMemoryStream increase the buffer
            int bufferLength = _fastMemoryStream.GetBuffer().Length * 2;

            byte[] byteArr = Enumerable.Repeat(val, bufferLength).ToArray();

            for (int i = 0; i < bufferLength; i++)
            {
                _fastMemoryStream.WriteByte(val);
            }

            Assert.Equal(byteArr, _fastMemoryStream.GetBuffer());
        }
    }
}
