using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Extensions;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public sealed class RestResponseExtensionsTest
    {
        [Test]
        public void TestRequestInProgressReturnsTrueForQueryInProgress()
        {
            var response = new NullDataResponse { code = 333333 };
            Assert.IsTrue(response.IsQueryInProgress());
        }

        [Test]
        public void TestRequestInProgressReturnsTrueForQueryInProgressAsync()
        {
            var response = new NullDataResponse { code = 333334 };
            Assert.IsTrue(response.IsQueryInProgress());
        }

        [Test]
        [TestCase(0)]
        [TestCase(200000)]
        [TestCase(390112)]
        [TestCase(333335)]
        [TestCase(333332)]
        [TestCase(-1)]
        public void TestRequestInProgressReturnsFalseForOtherCodes(int code)
        {
            var response = new NullDataResponse { code = code };
            Assert.IsFalse(response.IsQueryInProgress());
        }

        [Test]
        public void TestSessionExpiredReturnsTrueForExpiredCode()
        {
            var response = new NullDataResponse { code = 390112 };
            Assert.IsTrue(response.IsSessionExpired());
        }

        [Test]
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
            Assert.IsFalse(response.IsSessionExpired());
        }

        [Test]
        public void TestSessionNoLongerExistsReturnsTrueForNoLongerExistsCode()
        {
            var response = new NullDataResponse { code = 390111 };
            Assert.IsTrue(response.IsSessionGone());
        }

        [Test]
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
            Assert.IsFalse(response.IsSessionGone());
        }
    }
}
