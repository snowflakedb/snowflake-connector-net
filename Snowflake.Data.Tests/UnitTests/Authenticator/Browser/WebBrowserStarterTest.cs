using NUnit.Framework;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator.Browser;

namespace Snowflake.Data.Tests.UnitTests.Authenticator.Browser
{
    [TestFixture]
    public class WebBrowserStarterTest
    {
        [Test]
        public void TestRunUrlInBrowser()
        {
            // arrange
            var runner = new Mock<WebBrowserRunner>();
            var webBrowserStarter = new WebBrowserStarter(runner.Object);
            var validUrl = "http://localhost:8001/endpoint1";

            // act
            webBrowserStarter.StartBrowser(validUrl);

            // assert
            runner.Verify(r => r.Run(validUrl), Times.Once);
        }

        [Test]
        [TestCase("file:///home/user/index.html")]
        [TestCase("http://localhost:8001/endpoint!")]
        public void TestValidateUrl(string invalidUrl)
        {
            // arrange
            var runner = new Mock<WebBrowserRunner>();
            var webBrowserStarter = new WebBrowserStarter(runner.Object);

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => webBrowserStarter.StartBrowser(invalidUrl));

            // assert
            Assert.AreEqual(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            runner.Verify(r => r.Run(It.IsAny<string>()), Times.Never);
        }
    }
}
