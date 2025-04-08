using System;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests;

namespace Snowflake.Data.AuthenticationTests
{
    [NonParallelizable, IgnoreOnCI]
    public class PatTest
    {
        private string _connectionString = "";
        private string _patName = "";

        [SetUp, IgnoreOnCI]
        public void SetUp()
        {
            var parameters = AuthConnectionString.GetPatConnectionParameters();
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);

        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingPatSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetPatConnectionParameters();
            try
            {
                parameters[SFSessionProperty.TOKEN] = GetPat();
                _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
                authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            }
            finally
            {
                RemovePat();
            }
            authTestHelper.VerifyExceptionIsNotThrown();

        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingPatInvalid()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var parameters = AuthConnectionString.GetPatConnectionParameters();
            parameters[SFSessionProperty.TOKEN] = "invalidToken";
            _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsThrown("Programmatic access token is invalid.");
        }

        [Test, IgnoreOnCI]
        public void TestAuthenticateUsingPatMismatchedUser()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            authTestHelper.VerifyExceptionIsNotThrown();
            var parameters = AuthConnectionString.GetPatConnectionParameters();
            try
            {
                parameters[SFSessionProperty.TOKEN] = GetPat();
                parameters[SFSessionProperty.USER] = "differentUser";
                _connectionString = AuthConnectionString.ConvertToConnectionString(parameters);
                authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            }
            finally
            {
                RemovePat();
            }
            authTestHelper.VerifyExceptionIsThrown("Programmatic access token is invalid.");
        }

        private string GetPat()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            _patName = "PAT_DOTNET_" + GenerateRandomSuffix();
            var parameters = AuthConnectionString.GetPatConnectionParameters();
            string command = string.Format(
                "alter user {0} add programmatic access token {1} ROLE_RESTRICTION = '{2}' DAYS_TO_EXPIRY=1;",
                AuthConnectionString.SnowflakeUser, _patName, AuthConnectionString.SnowflakeRole);
            var patToken = authTestHelper.ConnectUsingOktaConnectionAndExecuteCustomCommand(command, true);
            return patToken;
        }

        private string GenerateRandomSuffix()
        {
            return DateTime.Now.ToString("yyyyMMddHHmmssfff");
        }

        private void RemovePat()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            string command = string.Format(
                "alter user {0} remove programmatic access token {1};",
                AuthConnectionString.SnowflakeUser, _patName);
            authTestHelper.ConnectUsingOktaConnectionAndExecuteCustomCommand(command);

        }
    }
}
