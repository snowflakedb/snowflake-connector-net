using System;
using NUnit.Framework;
using Moq;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class CrlRepositoryTest
    {
        const string CrlUrl = "http://snowflakecomputing.com/crl1.crl";
        private readonly Crl _crl = new Crl();

        [Test]
        public void TestGetCrlFromMemory()
        {
            // arrange
            var memoryCache = new Mock<ICrlCache>();
            memoryCache
                .Setup(c => c.Get(CrlUrl))
                .Returns(_crl);
            var fileCache = new Mock<ICrlCache>();
            var crlRepository = new CrlRepository(memoryCache.Object, fileCache.Object);

            // act
            var crl = crlRepository.Get(CrlUrl);

            // assert
            Assert.AreSame(_crl, crl);
            memoryCache.Verify(c => c.Get(CrlUrl), Times.Once);
            memoryCache.VerifyNoOtherCalls();
            fileCache.VerifyNoOtherCalls();
        }

        [Test]
        public void TestGetCrlFromFileCacheAndPropagateToMemory()
        {
            // arrange
            var memoryCache = new Mock<ICrlCache>();
            var fileCache = new Mock<ICrlCache>();
            fileCache
                .Setup(c => c.Get(CrlUrl))
                .Returns(_crl);
            var crlRepository = new CrlRepository(memoryCache.Object, fileCache.Object);

            // act
            var crl = crlRepository.Get(CrlUrl);

            // assert
            Assert.AreSame(_crl, crl);
            memoryCache.Verify(c => c.Get(CrlUrl), Times.Once);
            memoryCache.Verify(c => c.Set(CrlUrl, _crl), Times.Once);
            memoryCache.VerifyNoOtherCalls();
            fileCache.Verify(c => c.Get(CrlUrl), Times.Once);
            fileCache.VerifyNoOtherCalls();
        }

        [Test]
        public void TestSetCrl()
        {
            // arrange
            var memoryCache = new Mock<ICrlCache>();
            var fileCache = new Mock<ICrlCache>();
            var crlRepository = new CrlRepository(memoryCache.Object, fileCache.Object);

            // act
            crlRepository.Set(CrlUrl, _crl);

            // assert
            memoryCache.Verify(c => c.Set(CrlUrl, _crl), Times.Once);
            memoryCache.VerifyNoOtherCalls();
            fileCache.Verify(c => c.Set(CrlUrl, _crl), Times.Once);
            fileCache.VerifyNoOtherCalls();
        }

        [Test]
        [TestCase(true, true, typeof(MemoryCrlCache), typeof(FileCrlCache))]
        [TestCase(false, false, typeof(DisabledCrlCache), typeof(DisabledCrlCache))]
        [TestCase(true, false, typeof(MemoryCrlCache), typeof(DisabledCrlCache))]
        [TestCase(false, true, typeof(DisabledCrlCache), typeof(FileCrlCache))]
        public void TestConstructCaches(bool useMemoryCache, bool useFileCache, Type expectedMemoryCacheType, Type expectedFileCacheType)
        {
            // act
            var crlRepository = new CrlRepository(useMemoryCache, useFileCache);

            // assert
            Assert.AreEqual(expectedMemoryCacheType, crlRepository._memoryCrlCache.GetType());
            Assert.AreEqual(expectedFileCacheType, crlRepository._fileCrlCache.GetType());
        }
    }
}
