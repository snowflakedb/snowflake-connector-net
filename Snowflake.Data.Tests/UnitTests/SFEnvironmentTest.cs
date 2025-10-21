using System;
using Newtonsoft.Json;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFEnvironmentTest
    {
        [Test]
        public void TestRuntimeExtraction()
        {
            // Arrange
            string expectedRuntime = ".NET";
            string expectedVersion;

#if NETFRAMEWORK
            expectedRuntime += "Framework";
            expectedVersion = "4.8";
#elif NET6_0
            expectedVersion = "6.0";
#elif NET7_0
            expectedVersion = "7.0";
#elif NET8_0
            expectedVersion = "8.0";
#elif NET9_0
            expectedVersion = "9.0";
#endif

            // Act
            var actualRuntime = SFEnvironment.ExtractRuntime();
            var actualVersion = SFEnvironment.ExtractVersion();

            // Assert
            Assert.AreEqual(expectedRuntime, actualRuntime);
            Assert.AreEqual(expectedVersion, actualVersion);
        }

        [Test]
        public void TestApplicationPathExtraction()
        {
            // Act
            var applicationPath = SFEnvironment.ExtractApplicationPath();

            // Assert
            Assert.IsNotNull(applicationPath);
            Assert.IsNotEmpty(applicationPath);
            Assert.IsTrue(System.IO.Path.IsPathRooted(applicationPath),
                $"Application path should be absolute. Got: {applicationPath}");

            var lowerPath = applicationPath.ToLower();
            Assert.IsTrue(
                lowerPath.Contains("snowflake.data.tests") &&
                lowerPath.Contains("bin") &&
                lowerPath.Contains("testhost") &&
                (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")),
                $"Application path should contain 'snowflake.data.tests', 'bin', 'testhost' and end with .dll or .exe. Got: {applicationPath}");
        }

        [Test]
        public void TestClientEnvironmentDoesNotInterfereForDifferentAuthenticators()
        {
            // arrange/act
            var processName = System.Diagnostics.Process.GetCurrentProcess().ProcessName;
            var osVersion = Environment.OSVersion.VersionString;
            var netRuntime = SFEnvironment.ExtractRuntime();
            var netVersion = SFEnvironment.ExtractVersion();
            var applicationPath = SFEnvironment.ExtractApplicationPath();
            var clientCredentialAuthenticator = CreateAuthenticator("authenticator=oauth_client_credentials;account=test;db=testDb;role=testRole;oauthClientId=abc;oauthClientSecret=def;user=testUser;oauthTokenRequestUrl=https://test.snowflake.com;");
            ((OAuthClientCredentialsAuthenticator)clientCredentialAuthenticator).AccessToken = SecureStringHelper.Encode("qwe");
            var clientCredentialLoginClientEnv = clientCredentialAuthenticator.BuildLoginRequestData().clientEnv;
            var insecurePatAuthenticator = CreateAuthenticator("authenticator=programmatic_access_token;account=test;db=testDb;role=testRole;token=xyz;user=testUser;application=MyApp;certRevocationCheckMode=enabled;");
            var insecurePatLoginClientEnv = insecurePatAuthenticator.BuildLoginRequestData().clientEnv;
            var clientCredentialLoginClientEnv2 = clientCredentialAuthenticator.BuildLoginRequestData().clientEnv;

            // assert
            // asserts for client credential first login
            Assert.AreEqual(osVersion, clientCredentialLoginClientEnv.osVersion);
            Assert.AreEqual(netRuntime, clientCredentialLoginClientEnv.netRuntime);
            Assert.AreEqual(netVersion, clientCredentialLoginClientEnv.netVersion);
            Assert.AreEqual(processName, clientCredentialLoginClientEnv.application);
            Assert.AreEqual(processName, clientCredentialLoginClientEnv.processName);
            Assert.AreEqual(applicationPath, clientCredentialLoginClientEnv.applicationPath);
            Assert.AreEqual("disabled", clientCredentialLoginClientEnv.certRevocationCheckMode);
            Assert.AreEqual("oauth_client_credentials", clientCredentialLoginClientEnv.oauthType);
            // asserts for client credential second login
            Assert.AreEqual(osVersion, clientCredentialLoginClientEnv2.osVersion);
            Assert.AreEqual(netRuntime, clientCredentialLoginClientEnv2.netRuntime);
            Assert.AreEqual(netVersion, clientCredentialLoginClientEnv2.netVersion);
            Assert.AreEqual(processName, clientCredentialLoginClientEnv2.application);
            Assert.AreEqual(processName, clientCredentialLoginClientEnv2.processName);
            Assert.AreEqual(applicationPath, clientCredentialLoginClientEnv2.applicationPath);
            Assert.AreEqual("disabled", clientCredentialLoginClientEnv2.certRevocationCheckMode);
            Assert.AreEqual("oauth_client_credentials", clientCredentialLoginClientEnv2.oauthType);
            // asserts for PAT login
            Assert.AreEqual(osVersion, insecurePatLoginClientEnv.osVersion);
            Assert.AreEqual(netRuntime, insecurePatLoginClientEnv.netRuntime);
            Assert.AreEqual(netVersion, insecurePatLoginClientEnv.netVersion);
            Assert.AreEqual("MyApp", insecurePatLoginClientEnv.application);
            Assert.AreEqual(processName, insecurePatLoginClientEnv.processName);
            Assert.AreEqual(applicationPath, insecurePatLoginClientEnv.applicationPath);
            Assert.AreEqual("enabled", insecurePatLoginClientEnv.certRevocationCheckMode);
            Assert.IsNull(insecurePatLoginClientEnv.oauthType);
            // asserts that first and second client credential login produced the same json
            var firstClientCredentialEnvJson = JsonConvert.SerializeObject(clientCredentialLoginClientEnv);
            var secondClientCredentialEnvJson = JsonConvert.SerializeObject(clientCredentialLoginClientEnv2);
            Assert.AreEqual(firstClientCredentialEnvJson, secondClientCredentialEnvJson);
        }

        private BaseAuthenticator CreateAuthenticator(string connectionString)
        {
            var session = new SFSession(connectionString, new SessionPropertiesContext());
            session.InitialiseAuthenticator();
            return (BaseAuthenticator)session.authenticator;
        }
    }
}
