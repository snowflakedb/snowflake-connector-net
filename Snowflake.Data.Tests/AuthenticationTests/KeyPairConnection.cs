/*
 * Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
 */

using System;

using NUnit.Framework;



namespace Snowflake.Data.Tests.AuthenticationTests
{
    [NonParallelizable]
    public class KeyPairConnection : SFBaseTest
    {
        private string _connectionString = "";

        AuthTestHelper authTestHelper = new AuthTestHelper();

        [SetUp]
        public void SetUp()
         {


        }

        //TO BE UNCOMMENTED
        // [Test, Order(1)]
        // public void TestAuthenticateUsingKeyPairFilcContentSuccessful()
        // {
        //     var privateKey = AuthConnectionParameters.GetPriavteKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
        //     var parameters = AuthConnectionParameters.GetKeyPairFromFileContentParameters(privateKey);
        //     _connectionString = AuthConnectionParameters.SetPrivateKeyFromFileContentParameters(parameters);
        //
        //     Console.WriteLine(_connectionString);
        //     authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
        //     authTestHelper.verifyExceptionIsNotThrown();
        // }

        [Test, Order(2)]
        public void TestAuthenticateUsingKeyPairFileContentInvalidKey()
        {
            var privateKey = AuthConnectionParameters.GetPriavteKeyContentForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionParameters.GetKeyPairFromFileContentParameters(privateKey);
            _connectionString = AuthConnectionParameters.SetPrivateKeyFromFileContentParameters(parameters);

            Console.WriteLine(_connectionString);
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("Error: JWT token is invalid");
        }

        //TO BE UNCOMMENTED
        // [Test, Order(3)]
        // public void TestAuthenticateUsingKeyPairFilePathSuccessful()
        // {
        //     var privateKeyPath = AuthConnectionParameters.GetPriavteKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH");
        //     var parameters = AuthConnectionParameters.GetKeyPairFromFilePathParameters(privateKeyPath);
        //     _connectionString = AuthConnectionParameters.SetPrivateKeyFromFilePathParameters(parameters);
        //
        //     Console.WriteLine(_connectionString);
        //     authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
        //     authTestHelper.verifyExceptionIsNotThrown();
        // }

        [Test, Order(4)]
        public void TestAuthenticateUsingKeyPairFilePathInvalidKey()
        {
            var privateKeyPath = AuthConnectionParameters.GetPriavteKeyPathForKeypairAuth("SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH");
            var parameters = AuthConnectionParameters.GetKeyPairFromFilePathParameters(privateKeyPath);
            _connectionString = AuthConnectionParameters.SetPrivateKeyFromFilePathParameters(parameters);

            Console.WriteLine(_connectionString);
            authTestHelper.ConnectAndExecuteSimpleQuery(_connectionString);
            authTestHelper.verifyExceptionIsThrown("Error: JWT token is invalid");
        }
    }
}
