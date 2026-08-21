using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.AuthenticationTests
{
    public class MfaTest
    {
        [SFFact(SkipCondition.SkipOnCI)]
        public void TestAuthenticateUsingMfaSuccessful()
        {
            var parameters = AuthConnectionString.GetMfaConnectionString();
            var connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

            var authTestHelper = new AuthTestHelper();

            // Generate TOTP codes
            var totpCodes = authTestHelper.GetTotp();
            Assert.NotNull(totpCodes);
            Assert.True(totpCodes.Length > 0, $"Should have TOTP codes but got {totpCodes.Length}");

            // Test MFA authentication with TOTP codes
            var connectionSuccess = authTestHelper.ConnectAndExecuteSimpleQueryWithMfaToken(connectionString, totpCodes);
            Assert.True(connectionSuccess, $"Failed to connect with any of the {totpCodes.Length} TOTP codes");
            authTestHelper.VerifyExceptionIsNotThrown();

            // Test MFA token caching with second connection (without passcode)
            var cacheTestHelper = new AuthTestHelper();
            cacheTestHelper.ConnectAndExecuteSimpleQuery(connectionString);
            cacheTestHelper.VerifyExceptionIsNotThrown();

            var mfaHost = parameters[SFSessionProperty.HOST];
            authTestHelper.RemoveTokenFromCache(mfaHost, mfaHost, parameters[SFSessionProperty.USER], string.Empty, TokenType.MFAToken);
        }
    }
}
