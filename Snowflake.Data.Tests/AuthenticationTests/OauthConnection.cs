/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;

namespace Snowflake.Data.Tests.AuthenticationTests
{
    [NonParallelizable]
    public class OauthConnection : SFBaseTest
    {
        private string _connectionString = "";

        AuthTestHelper authTestHelper = new AuthTestHelper();

        [SetUp]
        public void SetUp()
        {
            string token = AuthConnectionParameters.GetOauthToken();
            var parameters = AuthConnectionParameters.GetOauthConnectionParameters(token);
            _connectionString = AuthConnectionParameters.SetOauthConnectionParameters(parameters);

        }

        [Test, Order(1)]
        public void TestAuthenticateUsingOauthSuccessful()
        {
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsNotThrown();
        }

        [Test, Order(2)]
        public void TestAuthenticateUsingOauthInvalidToken()
        {
            string token = "invalidToken";
            var parameters = AuthConnectionParameters.GetOauthConnectionParameters(token);
            _connectionString = AuthConnectionParameters.SetOauthConnectionParameters(parameters);


            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("Invalid OAuth access token");
        }

        [Test, Order(3), Ignore("Skipped, waits for SNOW-1893041")]
        public void TestAuthenticateUsingOauthMismatchedUser()
        {
            string token = AuthConnectionParameters.GetOauthToken();
            var parameters = AuthConnectionParameters.GetOauthConnectionParameters(token);
            parameters["user"] = "fakeAccount";
            _connectionString = AuthConnectionParameters.SetOauthConnectionParameters(parameters) + ";poolingEnabled=false;minPoolSize=0;";
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token");

        }
    }
}
