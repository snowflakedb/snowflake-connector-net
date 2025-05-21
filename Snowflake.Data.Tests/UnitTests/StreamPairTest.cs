using System.IO;
using NUnit.Framework;
using Moq;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class StreamPairTest
    {
        [Test]
        public void TestCloseBothStreams()
        {
            // arrange
            var mockedMainStream = new Mock<Stream>();
            var mockedHelperStream = new Mock<Stream>();

            // act
            using (new StreamPair { MainStream = mockedMainStream.Object, HelperStream = mockedHelperStream.Object }) { }

            // assert
            mockedMainStream.Verify(stream => stream.Close());
            mockedHelperStream.Verify(stream => stream.Close());
        }

        [Test]
        public void TestCloseMainStreamOnlyWhenHelperStreamNotGiven()
        {
            // arrange
            var mockedMainStream = new Mock<Stream>();

            // act
            using (new StreamPair { MainStream = mockedMainStream.Object }) { }

            // assert
            mockedMainStream.Verify(stream => stream.Close());
        }
    }
}
