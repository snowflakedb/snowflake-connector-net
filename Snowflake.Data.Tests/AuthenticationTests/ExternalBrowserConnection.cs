/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using NUnit.Framework;

namespace Snowflake.Data.Tests.AuthenticationTests
{
    [NonParallelizable]
    public class ExternalBrowserConnection : SFBaseTest
    {
        private string _connectionString = "";
        string _login = AuthConnectionParameters.SsoUser;
        string _password = AuthConnectionParameters.SsoPassword;
        AuthTestHelper authTestHelper = new AuthTestHelper();

        [SetUp]
        public void SetUp()
        {
            _login = AuthConnectionParameters.SsoUser;
            _password = AuthConnectionParameters.SsoPassword;
            authTestHelper.cleanBrowserProcess();
            var parameters = AuthConnectionParameters.GetExternalBrowserConnectionParameters();
            _connectionString = AuthConnectionParameters.SetExternalBrowserConnectionParameters(parameters);
        }

        [Test, Order(1)]
        public void TestAuthenticateUsingExternalBrowserSuccessful()
        {

            Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.getProvideCredentialsThread("success", _login, _password);

            authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.verifyExceptionIsNotThrown();

        }

        [Test, Order(2)]
        public void TestAuthenticateUsingExternalBrowserMismatchedUser()
        {
            var parameters = AuthConnectionParameters.GetExternalBrowserConnectionParameters();

            parameters["user"] = "differentUser";
            _connectionString = AuthConnectionParameters.SetExternalBrowserConnectionParameters(parameters);

            Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.getProvideCredentialsThread("success", _login, _password);

            authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.verifyExceptionIsThrown("The user you were trying to authenticate as differs from the user currently logged in at the IDP");

        }

    [Test, Order(3)]
    public void TestAuthenticateUsingExternalBrowserWrongCredentials()
    {
        var parameters = AuthConnectionParameters.GetExternalBrowserConnectionParameters();

        _connectionString = AuthConnectionParameters.SetExternalBrowserConnectionParameters(parameters);
        _connectionString += "BROWSER_RESPONSE_TIMEOUT=15;";

        _login = "itsnotanaccount.com";
        _password = "fakepassword";

        Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(_connectionString);
        Thread provideCredentialsThread = authTestHelper.getProvideCredentialsThread("fail", _login, _password);

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsThrown("Browser response timed out after 15 seconds");

    }

    [Test, Order(4)]
    public void TestAuthenticateUsingExternalBrowserTimeout()
    {
        var parameters = AuthConnectionParameters.GetExternalBrowserConnectionParameters();

        _connectionString = AuthConnectionParameters.SetExternalBrowserConnectionParameters(parameters);
        _connectionString += "BROWSER_RESPONSE_TIMEOUT=1;";

        Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(_connectionString);
        Thread provideCredentialsThread = authTestHelper.getProvideCredentialsThread("timeout", _login, _password);

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsThrown("Browser response timed out after 1 seconds");
    }
     }
}
