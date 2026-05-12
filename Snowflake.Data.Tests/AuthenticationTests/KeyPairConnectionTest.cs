using Xunit;
using Snowflake.Data.Tests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{
    public class KeyPairConnectionTest
    {
        private string _connectionString = "";

        [IgnoreOnCIFact]
        public void TestAuthenticateUsingKeyPairFileContentSuccessful()

        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var privateKey = AuthConnectionString.GetPrivateKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFileContentParameters(privateKey);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [IgnoreOnCIFact]
        public void TestAuthenticateUsingKeyPairFileContentInvalidKey()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var privateKey = AuthConnectionString.GetPrivateKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFileContentParameters(privateKey);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Error: JWT token is invalid");
        }

        [IgnoreOnCIFact]
        public void TestAuthenticateUsingKeyPairFilePathSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var privateKeyPath = AuthConnectionString.GetPrivateKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFilePathConnectionString(privateKeyPath);
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }

        [IgnoreOnCIFact]
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
