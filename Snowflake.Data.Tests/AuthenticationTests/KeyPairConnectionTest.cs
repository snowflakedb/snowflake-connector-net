using NUnit.Framework;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable, IgnoreOnCI]
    public class KeyPairConnectionTest
    {
        private string _connectionString = "";

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingKeyPairFileContentSuccessful()

        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var privateKey = AuthConnectionString.GetPrivateKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFileContentParameters(privateKey);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingKeyPairFileContentInvalidKey()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var privateKey = AuthConnectionString.GetPrivateKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFileContentParameters(privateKey);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Error: JWT token is invalid");
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingKeyPairFilePathSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var privateKeyPath = AuthConnectionString.GetPrivateKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFilePathConnectionString(privateKeyPath);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingKeyPairFilePathInvalidKey()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var privateKeyPath = AuthConnectionString.GetPrivateKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFilePathConnectionString(privateKeyPath);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Error: JWT token is invalid");
        }
    }
}
