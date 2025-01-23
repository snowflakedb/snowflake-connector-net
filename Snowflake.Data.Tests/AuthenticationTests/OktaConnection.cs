/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;

namespace Snowflake.Data.Tests.AuthenticationTests
{
    [NonParallelizable]
    public class OktaConnection : SFBaseTest
    {
        private string _connectionString = "";
        AuthTestHelper authTestHelper = new AuthTestHelper();

        [SetUp]
        public void SetUp()
        {
            var parameters = AuthConnectionParameters.GetOktaConnectionParameters();
            _connectionString = AuthConnectionParameters.SetOktaConnectionParameters(parameters);
            authTestHelper.cleanBrowserProcess();

        }

        [Test, Order(1)]
        public void TestAuthenticateUsingOktaSuccessful()
        {
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsNotThrown();

        }

        [Test, Order(2)]
        public void TestAuthenticateUsingOktaWrongUsernameParam()
        {
            var parameters = AuthConnectionParameters.GetOktaConnectionParameters();
            parameters["user"] = "differentUser";
            _connectionString = AuthConnectionParameters.SetOktaConnectionParameters(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("401 (Unauthorized)");
        }

        [Test, Order(3)]
        public void TestAuthenticateUsingOktaWrongCredentials()
        {
            var parameters = AuthConnectionParameters.GetOktaConnectionParameters();
            parameters["user"] = "differentUser";
            parameters["password"] = "fakepassword";

            _connectionString = AuthConnectionParameters.SetOktaConnectionParameters(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("401 (Unauthorized)");
        }

        [Test, Order(4)]
        public void TestAuthenticateUsingOktaWrongUrl()
        {
            var parameters = AuthConnectionParameters.GetOktaConnectionParameters();
            parameters["authenticator"] = "https://invalid.okta.com/";

            _connectionString = AuthConnectionParameters.SetOktaConnectionParameters(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("The specified authenticator is not accepted by your Snowflake account configuration");
        }


        [Test, Order(5)]
        public void TestAuthenticateUsingUrlWithoutOkta()
        {
            var parameters = AuthConnectionParameters.GetOktaConnectionParameters();
            parameters["authenticator"] = "https://invalid.abc.com/";

            _connectionString = AuthConnectionParameters.SetOktaConnectionParameters(parameters);

            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("Unknown authenticator");
        }
    }
}
