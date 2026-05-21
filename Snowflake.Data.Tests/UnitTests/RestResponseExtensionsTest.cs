using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Extensions;

namespace Snowflake.Data.Tests.UnitTests
{

    public sealed class RestResponseExtensionsTest
    {
        [SFFact]
        public void TestRequestInProgressReturnsTrueForQueryInProgress()
        {
            var response = new NullDataResponse { code = 333333 };
            Assert.True(response.IsQueryInProgress());
        }

        [SFFact]
        public void TestRequestInProgressReturnsTrueForQueryInProgressAsync()
        {
            var response = new NullDataResponse { code = 333334 };
            Assert.True(response.IsQueryInProgress());
        }

        [SFFact]
        [TestCase(0)]
        [TestCase(200000)]
        [TestCase(390112)]
        [TestCase(333335)]
        [TestCase(333332)]
        [TestCase(-1)]
        public void TestRequestInProgressReturnsFalseForOtherCodes(int code)
        {
            var response = new NullDataResponse { code = code };
            Assert.False(response.IsQueryInProgress());
        }

        [SFFact]
        public void TestSessionExpiredReturnsTrueForExpiredCode()
        {
            var response = new NullDataResponse { code = 390112 };
            Assert.True(response.IsSessionExpired());
        }

        [SFFact]
        [TestCase(0)]
        [TestCase(200000)]
        [TestCase(333333)]
        [TestCase(333334)]
        [TestCase(390111)]
        [TestCase(390113)]
        [TestCase(-1)]
        public void TestSessionExpiredReturnsFalseForOtherCodes(int code)
        {
            var response = new NullDataResponse { code = code };
            Assert.False(response.IsSessionExpired());
        }

        [SFFact]
        public void TestSessionNoLongerExistsReturnsTrueForNoLongerExistsCode()
        {
            var response = new NullDataResponse { code = 390111 };
            Assert.True(response.IsSessionGone());
        }

        [SFFact]
        [TestCase(0)]
        [TestCase(200000)]
        [TestCase(333333)]
        [TestCase(333334)]
        [TestCase(390110)]
        [TestCase(390112)]
        [TestCase(-1)]
        public void TestSessionNoLongerExistsReturnsFalseForOtherCodes(int code)
        {
            var response = new NullDataResponse { code = code };
            Assert.False(response.IsSessionGone());
        }
    }
}
