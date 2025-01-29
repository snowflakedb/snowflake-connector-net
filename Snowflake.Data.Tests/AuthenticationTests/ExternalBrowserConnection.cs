/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable]
    public class ExternalBrowserConnectionTest
    {
        private string _connectionString = "";
        private string _login = AuthConnectionString.SsoUser;
        private string _password = AuthConnectionString.SsoPassword;

        [SetUp]
        public void SetUp()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();
            _login = AuthConnectionString.SsoUser;
            _password = AuthConnectionString.SsoPassword;
            authTestHelper.CleanBrowserProcess();
            var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
            _connectionString = AuthConnectionString.SetExternalBrowserConnectionString(parameters);
        }

        [Test]
        public void TestAuthenticateUsingExternalBrowserSuccessful()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsNotThrown();

        }

        [Test]
        public void TestAuthenticateUsingExternalBrowserMismatchedUser()
        {
            AuthTestHelper authTestHelper = new AuthTestHelper();

            var parameters = AuthConnectionString.GetExternalBrowserConnectionString();
            parameters[SFSessionProperty.USER] = "differentUser";

            _connectionString = AuthConnectionString.SetExternalBrowserConnectionString(parameters);

            Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
            Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("success", _login, _password);

            authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
            authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user currently logged in at the IDP");

        }

    [Test]
    public void TestAuthenticateUsingExternalBrowserWrongCredentials()
    {
        AuthTestHelper authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetExternalBrowserConnectionString();

        _connectionString = AuthConnectionString.SetExternalBrowserConnectionString(parameters);
        _connectionString += "BROWSER_RESPONSE_TIMEOUT=15;";

        _login = "itsnotanaccount.com";
        _password = "fakepassword";

        Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("fail", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 15 seconds");

    }

    [Test]
    public void TestAuthenticateUsingExternalBrowserTimeout()
    {
        AuthTestHelper authTestHelper = new AuthTestHelper();

        var parameters = AuthConnectionString.GetExternalBrowserConnectionString();

        _connectionString = AuthConnectionString.SetExternalBrowserConnectionString(parameters);
        _connectionString += "BROWSER_RESPONSE_TIMEOUT=1;";

        Thread connectThread = authTestHelper.GetConnectAndExecuteSimpleQueryThread(_connectionString);
        Thread provideCredentialsThread = authTestHelper.GetProvideCredentialsThread("timeout", _login, _password);

        authTestHelper.ConnectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.VerifyExceptionIsThrown("Browser response timed out after 1 seconds");
    }
      }
}
