/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using System;
using NUnit.Framework;
namespace Snowflake.Data.AuthenticationTests
{

    [NonParallelizable]
    public class KeyPairConnectionTest
    {
        private string _connectionString = "";


        [Test]
        public void TestAuthenticateUsingKeyPairFileContentSuccessful()

        {
            Console.WriteLine("Before test, after setup");
            AuthTestHelper authTestHelper = new AuthTestHelper();
            var privateKey = AuthConnectionString.GetPrivateKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionString.GetKeyPairFromFileContentParameters(privateKey);
            _connectionString = AuthConnectionString.SetPrivateKeyFromFileContentConnectionString(parameters);
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.VerifyExceptionIsNotThrown();
        }
//
//          [Test, IgnoreOnCI]
//          public void TestAuthenticateUsingKeyPairFileContentInvalidKey()
//          {
//              AuthTestHelper authTestHelper = new AuthTestHelper();
//
//              var privateKey = AuthConnectionString.GetPrivateKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
//              var parameters = AuthConnectionString.GetKeyPairFromFileContentParameters(privateKey);
//              _connectionString = AuthConnectionString.SetPrivateKeyFromFileContentConnectionString(parameters);
//
//              authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
//              authTestHelper.VerifyExceptionIsThrown("Error: JWT token is invalid");
//          }
//
//           [Test, IgnoreOnCI]
//           public void TestAuthenticateUsingKeyPairFilePathSuccessful()
//           {
//                AuthTestHelper authTestHelper = new AuthTestHelper();
//                var privateKeyPath = AuthConnectionString.GetPrivateKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
//                var parameters = AuthConnectionString.GetKeyPairFromFilePathConnectionString(privateKeyPath);
//                _connectionString = AuthConnectionString.SetPrivateKeyFromFilePathConnectionString(parameters);
//
//                authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
//                authTestHelper.VerifyExceptionIsNotThrown();
//            }
//
//           [Test, IgnoreOnCI]
//           public void TestAuthenticateUsingKeyPairFilePathInvalidKey()
//           {
//               AuthTestHelper authTestHelper = new AuthTestHelper();
//
//               var privateKeyPath = AuthConnectionString.GetPrivateKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
//               var parameters = AuthConnectionString.GetKeyPairFromFilePathConnectionString(privateKeyPath);
//               _connectionString = AuthConnectionString.SetPrivateKeyFromFilePathConnectionString(parameters);
//
//               authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
//               authTestHelper.VerifyExceptionIsThrown("Error: JWT token is invalid");
//           }
     }
}
