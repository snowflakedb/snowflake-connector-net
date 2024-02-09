using System;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core.Authenticator.Okta;

namespace Snowflake.Data.Tests.UnitTests.Core.Authenticator.Okta
{
    public class SamlRestRequestFactoryTests
    {
        private ISamlRestRequestFactory _samlRestRequestFactory;
        
        [SetUp]
        public void SetUp()
        {
            _samlRestRequestFactory = new SamlRestRequestFactory();
        }

        [Test]
        public void TestIfCorrectSamlRestRequestIsCreated()
        {
            // arrange
            var uri = new Uri("https://test.com");
            var onetimeToken = Guid.NewGuid().ToString();
            var timeout = TimeSpan.Parse("00:10:00");
            
            // act
            var actual = _samlRestRequestFactory.Create(uri, onetimeToken, timeout);

            // assert
            Assert.AreEqual(uri, actual.Url);
            Assert.AreEqual(timeout, actual.RestTimeout);
            Assert.AreEqual(Timeout.InfiniteTimeSpan, actual.HttpTimeout);
            Assert.AreEqual(onetimeToken, actual.OnetimeToken);
        }
    }
}