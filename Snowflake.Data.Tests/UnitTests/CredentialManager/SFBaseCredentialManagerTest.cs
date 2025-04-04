using NUnit.Framework;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public abstract class SFBaseCredentialManagerTest
    {
        protected ISnowflakeCredentialManager _credentialManager;

        [Test]
        public void TestSavingAndRemovingCredentials()
        {
            // arrange
            var key = "mockKey";
            var expectedToken = "token";

            // act
            _credentialManager.SaveCredentials(key, expectedToken);

            // assert
            Assert.AreEqual(expectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        [Test]
        public void TestSavingCredentialsForAnExistingKey()
        {
            // arrange
            var key = "mockKey";
            var firstExpectedToken = "mockToken1";
            var secondExpectedToken = "mockToken2";

            // act
            _credentialManager.SaveCredentials(key, firstExpectedToken);

            // assert
            Assert.AreEqual(firstExpectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.SaveCredentials(key, secondExpectedToken);

            // assert
            Assert.AreEqual(secondExpectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));

        }

        [Test]
        public void TestRemovingCredentialsForKeyThatDoesNotExist()
        {
            // arrange
            var key = "mockKey";

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        [Test]
        public void TestGetCredentialsForProperKey()
        {
            // arrange
            var key = "key";
            var anotherKey = "anotherKey";
            var token = "token";
            var anotherToken = "anotherToken";
            _credentialManager.SaveCredentials(key, token);
            _credentialManager.SaveCredentials(anotherKey, anotherToken);

            // act
            var result = _credentialManager.GetCredentials(key);

            // assert
            Assert.AreEqual(token, result);
        }

        [Test]
        public void TestGetCredentialsForTokenWithManyCharacters()
        {
            // arrange
            var key = "mockKey";
            var expectedToken = "access-token-123";

            // act
            _credentialManager.SaveCredentials(key, expectedToken);

            // assert
            Assert.AreEqual(expectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        [Test]
        public void TestGetCredentialsForCredentialsThatDoesNotExist()
        {
            // arrange
            var key = "fakeKey";

            // act
            _credentialManager.RemoveCredentials(key);
            var token = _credentialManager.GetCredentials(key);

            // assert
            Assert.IsTrue(string.IsNullOrEmpty(token));
        }
    }
}
