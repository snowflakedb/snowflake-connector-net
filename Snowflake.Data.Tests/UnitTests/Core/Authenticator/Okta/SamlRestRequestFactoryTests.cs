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
            
            // act
            var actual = _samlRestRequestFactory.Create();

            // assert
            
        }
    }
}