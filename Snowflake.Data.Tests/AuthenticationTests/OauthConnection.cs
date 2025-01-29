/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable]
    public class OauthConnectionTest
    {
        private string _connectionString = "";

        [SetUp]
        public void SetUp()
        {
            string token = AuthConnectionString.GetOauthToken();
            var parameters = AuthConnectionString.GetOauthConnectionString(token);
            _connectionString = AuthConnectionString.SetOauthConnectionString(parameters);

        }

         [Test]
         public void TestAuthenticateUsingOauthSuccessful()
         {
             AuthTestHelper authTestHelper = new AuthTestHelper();

             authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
             authTestHelper.VerifyExceptionIsNotThrown();
         }

         [Test]
         public void TestAuthenticateUsingOauthInvalidToken()
         {
             AuthTestHelper authTestHelper = new AuthTestHelper();

             string token = "invalidToken";
             var parameters = AuthConnectionString.GetOauthConnectionString(token);
             _connectionString = AuthConnectionString.SetOauthConnectionString(parameters);

             authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
             authTestHelper.VerifyExceptionIsThrown("Invalid OAuth access token");
         }

         [Test, Ignore("Skipped, waits for SNOW-1893041")]
         public void TestAuthenticateUsingOauthMismatchedUser()
         {
             AuthTestHelper authTestHelper = new AuthTestHelper();

             string token = AuthConnectionString.GetOauthToken();
             var parameters = AuthConnectionString.GetOauthConnectionString(token);
             parameters[SFSessionProperty.USER] = "fakeAccount";
             _connectionString = AuthConnectionString.SetOauthConnectionString(parameters) + ";poolingEnabled=false;minPoolSize=0;";

             authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
             authTestHelper.VerifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token");

         }
    }
}
