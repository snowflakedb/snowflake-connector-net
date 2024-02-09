using System;
using System.Security;
using NUnit.Framework;
using Snowflake.Data.Core.Authenticator.Okta;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Core.Authenticator.Okta
{
    public class IdpTokenRestRequestFactoryTests
    {
        private IdpTokenRestRequestFactory _idpTokenRestRequestFactory;

        [SetUp]
        public void SetUp()
        {
            _idpTokenRestRequestFactory = new IdpTokenRestRequestFactory();
        }

        [Test]
        public void TestIfCreateCorrectRequest()
        {
            // arrange
            var tokenUrl = new Uri("https://test.com/");
            var session = new SFSession("ACCOUNT=account;USER=username1;",  new SecureString());
            
            // act
            var actual = _idpTokenRestRequestFactory.Create(tokenUrl, session);

            // assert
            Assert.AreEqual(tokenUrl, actual.Url);
            Assert.AreEqual(session.connectionTimeout, actual.RestTimeout);
            Assert.AreEqual(TimeSpan.FromSeconds(16), actual.HttpTimeout);
            Assert.AreEqual("username1", actual.JsonBody.Username);
            Assert.AreEqual("", actual.JsonBody.Password);
        }
    }
}