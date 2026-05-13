using Xunit;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public abstract class SFBaseCredentialManagerTest
    {
        protected ISnowflakeCredentialManager _credentialManager;

        public virtual void TestSavingAndRemovingCredentials()
        {
            // arrange
            var key = "mockKey";
            var expectedToken = "token";

            // act
            _credentialManager.SaveCredentials(key, expectedToken);

            // assert
            Assert.Equal(expectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.True(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        public virtual void TestSavingCredentialsForAnExistingKey()
        {
            // arrange
            var key = "mockKey";
            var firstExpectedToken = "mockToken1";
            var secondExpectedToken = "mockToken2";

            // act
            _credentialManager.SaveCredentials(key, firstExpectedToken);

            // assert
            Assert.Equal(firstExpectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.SaveCredentials(key, secondExpectedToken);

            // assert
            Assert.Equal(secondExpectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.True(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));

        }

        public virtual void TestRemovingCredentialsForKeyThatDoesNotExist()
        {
            // arrange
            var key = "mockKey";

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.True(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        public virtual void TestGetCredentialsForProperKey()
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
            Assert.Equal(token, result);
        }

        public virtual void TestGetCredentialsForTokenWithManyCharacters()
        {
            // arrange
            var key = "mockKey";
            var expectedToken = "access-token-123";

            // act
            _credentialManager.SaveCredentials(key, expectedToken);

            // assert
            Assert.Equal(expectedToken, _credentialManager.GetCredentials(key));

            // act
            _credentialManager.RemoveCredentials(key);

            // assert
            Assert.True(string.IsNullOrEmpty(_credentialManager.GetCredentials(key)));
        }

        public virtual void TestGetCredentialsForCredentialsThatDoesNotExist()
        {
            // arrange
            var key = "fakeKey";

            // act
            _credentialManager.RemoveCredentials(key);
            var token = _credentialManager.GetCredentials(key);

            // assert
            Assert.True(string.IsNullOrEmpty(token));
        }
    }
}
