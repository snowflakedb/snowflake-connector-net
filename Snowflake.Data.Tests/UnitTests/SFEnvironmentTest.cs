using System;
using System.IO;
using System.Runtime.InteropServices;
using Newtonsoft.Json;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests
{

    public sealed class SFEnvironmentTest
    {
        [SFFact]
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
#elif NET10_0
            expectedVersion = "10.0";
#endif

            // Act
            var actualRuntime = SFEnvironment.ExtractRuntime();
            var actualVersion = SFEnvironment.ExtractVersion();

            // Assert
            Assert.Equal(expectedRuntime, actualRuntime);
            Assert.Equal(expectedVersion, actualVersion);
        }

        [SFFact]
        [RunOnlyOnCI]
        public void TestApplicationPathExtraction()
        {
            var applicationPath = SFEnvironment.ExtractApplicationPath();

            Assert.NotNull(applicationPath);
            Assert.NotEmpty(applicationPath);
            Assert.True(System.IO.Path.IsPathRooted(applicationPath),
                $"Application path should be absolute. Got: {applicationPath}");

            var lowerPath = applicationPath.ToLower();
#if NETFRAMEWORK
            Assert.True(
                lowerPath.Contains("testhost") &&
                (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")),
                $"Application path should contain 'testhost' and end with .dll or .exe. Got: {applicationPath}");
#else
            Assert.True(
                lowerPath.Contains("snowflake.data.tests") &&
                lowerPath.Contains("bin") &&
                lowerPath.Contains("testhost") &&
                (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")),
                $"Application path should contain 'snowflake.data.tests', 'bin', 'testhost' and end with .dll or .exe. Got: {applicationPath}");
#endif
        }

        [SFFact]
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
            Assert.Equal(osVersion, clientCredentialLoginClientEnv.osVersion);
            Assert.Equal(netRuntime, clientCredentialLoginClientEnv.netRuntime);
            Assert.Equal(netVersion, clientCredentialLoginClientEnv.netVersion);
            Assert.Equal(processName, clientCredentialLoginClientEnv.application);
            Assert.Equal(processName, clientCredentialLoginClientEnv.processName);
            Assert.Equal(applicationPath, clientCredentialLoginClientEnv.applicationPath);
            Assert.Equal("disabled", clientCredentialLoginClientEnv.certRevocationCheckMode);
            Assert.Equal("oauth_client_credentials", clientCredentialLoginClientEnv.oauthType);
            // asserts for client credential second login
            Assert.Equal(osVersion, clientCredentialLoginClientEnv2.osVersion);
            Assert.Equal(netRuntime, clientCredentialLoginClientEnv2.netRuntime);
            Assert.Equal(netVersion, clientCredentialLoginClientEnv2.netVersion);
            Assert.Equal(processName, clientCredentialLoginClientEnv2.application);
            Assert.Equal(processName, clientCredentialLoginClientEnv2.processName);
            Assert.Equal(applicationPath, clientCredentialLoginClientEnv2.applicationPath);
            Assert.Equal("disabled", clientCredentialLoginClientEnv2.certRevocationCheckMode);
            Assert.Equal("oauth_client_credentials", clientCredentialLoginClientEnv2.oauthType);
            // asserts for PAT login
            Assert.Equal(osVersion, insecurePatLoginClientEnv.osVersion);
            Assert.Equal(netRuntime, insecurePatLoginClientEnv.netRuntime);
            Assert.Equal(netVersion, insecurePatLoginClientEnv.netVersion);
            Assert.Equal("MyApp", insecurePatLoginClientEnv.application);
            Assert.Equal(processName, insecurePatLoginClientEnv.processName);
            Assert.Equal(applicationPath, insecurePatLoginClientEnv.applicationPath);
            Assert.Equal("enabled", insecurePatLoginClientEnv.certRevocationCheckMode);
            Assert.Null(insecurePatLoginClientEnv.oauthType);
            // asserts that first and second client credential login produced the same json
            var firstClientCredentialEnvJson = JsonConvert.SerializeObject(clientCredentialLoginClientEnv);
            var secondClientCredentialEnvJson = JsonConvert.SerializeObject(clientCredentialLoginClientEnv2);
            Assert.Equal(firstClientCredentialEnvJson, secondClientCredentialEnvJson);
        }

        [SFFact]
        [Platform("Linux")]
        public void TestOsDetailsExtractionOnLinux()
        {
            var osDetails = SFEnvironment.ExtractOsDetails();

            if (osDetails == null)
            {
                Assert.False(File.Exists("/etc/os-release"),
                    "ExtractOsDetails returned null but /etc/os-release exists");
                return;
            }

            Assert.NotEmpty(osDetails);
            var expectedKeys = new[] { "NAME", "PRETTY_NAME", "ID", "BUILD_ID", "IMAGE_ID", "IMAGE_VERSION", "VERSION", "VERSION_ID" };
            foreach (var key in osDetails.Keys)
            {
                Assert.Contains(key, expectedKeys, $"Unexpected key '{key}' found in OS details");
            }
        }

        [SFFact]
        public void TestStaticConstructorSetsClientEnvCorrectly()
        {
            var clientEnv = SFEnvironment.ClientEnv;

            // Fields set by static constructor
            Assert.Equal(RuntimeInformation.ProcessArchitecture.ToString().ToLower(), clientEnv.isa);

            // Fields NOT set by static constructor (populated in CloneForSession)
            Assert.Null(clientEnv.minicoreVersion, "minicoreVersion should be null on the static ClientEnv");
            Assert.Null(clientEnv.minicoreFileName, "minicoreFileName should be null on the static ClientEnv");
            Assert.Null(clientEnv.minicoreLoadError, "minicoreLoadError should be null on the static ClientEnv");
            Assert.Null(clientEnv.platform, "platform should be null on the static ClientEnv");
        }

        [SFFact]
        [Platform("Linux")]
        public void TestStaticConstructorSetsLibcFieldsOnLinux()
        {
            var clientEnv = SFEnvironment.ClientEnv;

            Assert.NotNull(clientEnv.libcFamily, "libcFamily should not be null on Linux");
            Assert.That(clientEnv.libcFamily, Is.AnyOf("glibc", "could not determine"));

            Assert.NotNull(clientEnv.libcVersion, "libcVersion should not be null when family is glibc");
            Assert.That(clientEnv.libcVersion, Does.Match(@"^\d+\.\d+"),
                $"libcVersion should be a version string, got: {clientEnv.libcVersion}");
        }

        [SFFact]
        [Platform(Exclude = "Linux")]
        public void TestStaticConstructorSetsLibcFieldsOnNonLinux()
        {
            var clientEnv = SFEnvironment.ClientEnv;

            Assert.Null(clientEnv.libcFamily, "libcFamily should be null on non-Linux");
            Assert.Null(clientEnv.libcVersion, "libcVersion should be null on non-Linux");
        }

        [SFFact]
        [Platform("Linux")]
        public void TestStaticConstructorSetsOsDetailsOnLinux()
        {
            var clientEnv = SFEnvironment.ClientEnv;

            if (clientEnv.osDetails == null)
            {
                Assert.False(File.Exists("/etc/os-release"),
                    "osDetails should not be null when /etc/os-release exists");
                return;
            }

            Assert.NotEmpty(clientEnv.osDetails);
        }

        [SFFact]
        [Platform(Exclude = "Linux")]
        public void TestStaticConstructorSetsOsDetailsOnNonLinux()
        {
            var clientEnv = SFEnvironment.ClientEnv;

            Assert.Null(clientEnv.osDetails, "osDetails should be null on non-Linux");
        }

        [SFFact]
        [Platform(Exclude = "Linux")]
        public void TestOsDetailsExtractionOnNonLinux()
        {
            var osDetails = SFEnvironment.ExtractOsDetails();
            Assert.Null(osDetails, "OS details should be null on non-Linux platforms");
        }

        [SFFact]
        [Platform("Linux")]
        public void TestOsDetailsFiltersUnwantedKeys()
        {
            var osDetails = SFEnvironment.ExtractOsDetails();

            if (osDetails == null)
            {
                Assert.False(File.Exists("/etc/os-release"),
                    "ExtractOsDetails returned null but /etc/os-release exists");
                return;
            }

            var unwantedKeys = new[] { "ANSI_COLOR", "HOME_URL", "DOCUMENTATION_URL", "SUPPORT_URL", "BUG_REPORT_URL", "PRIVACY_POLICY_URL", "LOGO" };
            foreach (var unwantedKey in unwantedKeys)
            {
                Assert.False(osDetails.ContainsKey(unwantedKey),
                    $"OS details should not contain unwanted key '{unwantedKey}'");
            }
        }

        private BaseAuthenticator CreateAuthenticator(string connectionString)
        {
            var session = new SFSession(connectionString, new SessionPropertiesContext());
            session.InitialiseAuthenticator();
            return (BaseAuthenticator)session.authenticator;
        }
    }
}
