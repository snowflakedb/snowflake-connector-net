using System;
using NUnit.Framework;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    [TestFixture]
    public class MemoryCrlCacheTests
    {
        const string CrlUrl1 = "http://snowflakecomputing.com/crl1.crl";
        const string CrlUrl2 = "http://snowflakecomputing.com/crl2.crl";
        private readonly Crl _crl1 = new();
        private readonly Crl _crl2 = new();

        [Test]
        public void TestCacheOperations()
        {
            var cache = new MemoryCrlCache(TimeSpan.FromDays(1));
            Assert.Null(cache.Get(CrlUrl1));
            Assert.Null(cache.Get(CrlUrl2));
            cache.Set(CrlUrl1, _crl1);
            Assert.AreSame(_crl1, cache.Get(CrlUrl1));
            Assert.Null(cache.Get(CrlUrl2));
            cache.Set(CrlUrl1, _crl2);
            Assert.AreSame(_crl2, cache.Get(CrlUrl1));
            Assert.Null(cache.Get(CrlUrl2));
            cache.Set(CrlUrl2, _crl1);
            Assert.AreSame(_crl2, cache.Get(CrlUrl1));
            Assert.AreSame(_crl1, cache.Get(CrlUrl2));
        }
    }
}
