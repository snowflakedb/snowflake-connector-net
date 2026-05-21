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

        [SFTheory]
        [InlineData(0)]
        [InlineData(200000)]
        [InlineData(390112)]
        [InlineData(333335)]
        [InlineData(333332)]
        [InlineData(-1)]
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

        [SFTheory]
        [InlineData(0)]
        [InlineData(200000)]
        [InlineData(333333)]
        [InlineData(333334)]
        [InlineData(390111)]
        [InlineData(390113)]
        [InlineData(-1)]
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

        [SFTheory]
        [InlineData(0)]
        [InlineData(200000)]
        [InlineData(333333)]
        [InlineData(333334)]
        [InlineData(390110)]
        [InlineData(390112)]
        [InlineData(-1)]
        public void TestSessionNoLongerExistsReturnsFalseForOtherCodes(int code)
        {
            var response = new NullDataResponse { code = code };
            Assert.False(response.IsSessionGone());
        }
    }
}
