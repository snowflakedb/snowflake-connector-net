using System.IO;
using Xunit;
using Moq;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class StreamPairTest
    {
        [SFFact]
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

        [SFFact]
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
