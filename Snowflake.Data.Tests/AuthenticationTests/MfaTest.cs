//using NUnit.Framework;
//using Snowflake.Data.Tests;
//using Snowflake.Data.Core;
//using Snowflake.Data.Core.CredentialManager;

//namespace Snowflake.Data.AuthenticationTests
//{
//    [NonParallelizable, IgnoreOnCI]
//    public class MfaTest
//    {
//        [Test, IgnoreOnCI]
//        public void TestAuthenticateUsingMfaSuccessful()
//        {
//            var parameters = AuthConnectionString.GetMfaConnectionString();
//            var connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

//            var authTestHelper = new AuthTestHelper();

//            // Generate TOTP codes
//            var totpCodes = authTestHelper.GetTotp();
//            Assert.IsNotNull(totpCodes, "TOTP codes should not be null");
//            Assert.IsTrue(totpCodes.Length > 0, $"Should have TOTP codes but got {totpCodes.Length}");

//            // Test MFA authentication with TOTP codes
//            var connectionSuccess = authTestHelper.ConnectAndExecuteSimpleQueryWithMfaToken(connectionString, totpCodes);
//            Assert.IsTrue(connectionSuccess, $"Failed to connect with any of the {totpCodes.Length} TOTP codes");
//            authTestHelper.VerifyExceptionIsNotThrown();

//            // Test MFA token caching with second connection (without passcode)
//            var cacheTestHelper = new AuthTestHelper();
//            cacheTestHelper.ConnectAndExecuteSimpleQuery(connectionString);
//            cacheTestHelper.VerifyExceptionIsNotThrown();

//            authTestHelper.RemoveTokenFromCache(parameters[SFSessionProperty.HOST], parameters[SFSessionProperty.USER], TokenType.MFAToken);
//        }
//    }
//}
