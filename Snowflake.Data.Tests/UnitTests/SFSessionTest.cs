using Newtonsoft.Json;
using Snowflake.Data.Core;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFSessionTest
    {
        // Mock test for session gone
        [Test]
        public void TestSessionGoneWhenClose()
        {
            var restRequester = new MockCloseSessionGone();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            Assert.DoesNotThrow(() => sfSession.close());
        }

        [Test]
        public void TestSessionGoneWhenCloseNonBlocking()
        {
            var restRequester = new MockCloseSessionGone();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            Assert.DoesNotThrow(() => sfSession.CloseNonBlocking());
        }

        [Test]
        public void TestUpdateSessionProperties()
        {
            // arrange
            string databaseName = "DB_TEST";
            string schemaName = "SC_TEST";
            string warehouseName = "WH_TEST";
            string roleName = "ROLE_TEST";
            QueryExecResponseData queryExecResponseData = new QueryExecResponseData
            {
                finalSchemaName = schemaName,
                finalDatabaseName = databaseName,
                finalRoleName = roleName,
                finalWarehouseName = warehouseName
            };

            // act
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null);
            sfSession.UpdateSessionProperties(queryExecResponseData);

            // assert
            Assert.AreEqual(databaseName, sfSession.database);
            Assert.AreEqual(schemaName, sfSession.schema);
            Assert.AreEqual(warehouseName, sfSession.warehouse);
            Assert.AreEqual(roleName, sfSession.role);
        }

        [Test]
        public void TestSkipUpdateSessionPropertiesWhenPropertiesMissing()
        {
            // arrange
            string databaseName = "DB_TEST";
            string schemaName = "SC_TEST";
            string warehouseName = "WH_TEST";
            string roleName = "ROLE_TEST";
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null);
            sfSession.database = databaseName;
            sfSession.warehouse = warehouseName;
            sfSession.role = roleName;
            sfSession.schema = schemaName;

            // act
            QueryExecResponseData queryExecResponseWithoutData = new QueryExecResponseData();
            sfSession.UpdateSessionProperties(queryExecResponseWithoutData);

            // assert
            // when database or schema name is missing in the response,
            // the cached value should keep unchanged
            Assert.AreEqual(databaseName, sfSession.database);
            Assert.AreEqual(schemaName, sfSession.schema);
            Assert.AreEqual(warehouseName, sfSession.warehouse);
            Assert.AreEqual(roleName, sfSession.role);
        }

        [Test]
        [TestCase(null)]
        [TestCase("/some-path/config.json")]
        [TestCase("C:\\some-path\\config.json")]
        public void TestThatConfiguresEasyLogging(string configPath)
        {
            // arrange
            var easyLoggingStarter = new Moq.Mock<EasyLoggingStarter>();
            var simpleConnectionString = "account=test;user=test;password=test;";
            var connectionString = configPath == null
                ? simpleConnectionString
                : $"{simpleConnectionString}client_config_file={configPath};";

            // act
            new SFSession(connectionString, null, null, easyLoggingStarter.Object);

            // assert
            easyLoggingStarter.Verify(starter => starter.Init(configPath));
        }

        [TestCase(null, "accountDefault", "accountDefault", false)]
        [TestCase("initial", "initial", "initial", false)]
        [TestCase("initial", null, "initial", false)]
        [TestCase("initial", "IniTiaL", "initial", false)]
        [TestCase("initial", "final", "final", true)]
        [TestCase("initial", "\\\"final\\\"", "\"final\"", true)]
        [TestCase("initial", "\\\"Final\\\"", "\"Final\"", true)]
        [TestCase("\"Ini\\t\"ial\"", "\\\"Ini\\t\"ial\\\"", "\"Ini\\t\"ial\"", false)]
        [TestCase("\"initial\"", "initial", "initial", true)]
        [TestCase("\"initial\"", "\\\"initial\\\"", "\"initial\"", false)]
        [TestCase("init\"ial", "init\"ial", "init\"ial", false)]
        [TestCase("\"init\"ial\"", "\\\"init\"ial\\\"", "\"init\"ial\"", false)]
        [TestCase("\"init\"ial\"", "\\\"Init\"ial\\\"", "\"Init\"ial\"", true)]
        public void TestSessionPropertyQuotationSafeUpdateOnServerResponse(string sessionInitialValue, string serverResponseFinalSessionValue, string unquotedExpectedFinalValue, bool wasChanged)
        {
            // Arrange
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null);
            var changedSessionValue = sessionInitialValue;

            // Act
            sfSession.UpdateSessionProperty(ref changedSessionValue, serverResponseFinalSessionValue);

            // Assert
            Assert.AreEqual(sfSession.SessionPropertiesChanged, wasChanged);
            if (wasChanged || sessionInitialValue is null)
                Assert.AreEqual(unquotedExpectedFinalValue, changedSessionValue);
            else
                Assert.AreEqual(sessionInitialValue, changedSessionValue);
        }

        [Test]
        public void TestHandlePasswordWithQuotations()
        {
            // arrange
            MockLoginStoringRestRequester restRequester = new MockLoginStoringRestRequester();
            SFSession sfSession = new SFSession("account=test;user=test;password=test\"with'quotations{}", null, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests[0];
            Assert.AreEqual("test\"with'quotations{}", loginRequest.data.password);

            // act
            var json = JsonConvert.SerializeObject(loginRequest, JsonUtils.JsonSettings);
            var deserializedLoginRequest = (LoginRequest) JsonConvert.DeserializeObject(json, typeof(LoginRequest));

            // assert
            Assert.AreEqual(loginRequest.data.password, deserializedLoginRequest.data.password);
        }

        [Test]
        public void TestHandlePasscodeParameter()
        {
            // arrange
            var passcode = "123456";
            MockLoginStoringRestRequester restRequester = new MockLoginStoringRestRequester();
            SFSession sfSession = new SFSession($"account=test;user=test;password=test;passcode={passcode}", null, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests[0];
            Assert.AreEqual(passcode, loginRequest.data.passcode);
            Assert.AreEqual("passcode", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestHandlePasscodeAsSecureString()
        {
            // arrange
            var passcode = "123456";
            MockLoginStoringRestRequester restRequester = new MockLoginStoringRestRequester();
            SFSession sfSession = new SFSession($"account=test;user=test;password=test;", null, SecureStringHelper.Encode(passcode), EasyLoggingStarter.Instance, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests[0];
            Assert.AreEqual(passcode, loginRequest.data.passcode);
            Assert.AreEqual("passcode", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestHandlePasscodeInPasswordParameter()
        {
            // arrange
            var passcode = "123456";
            MockLoginStoringRestRequester restRequester = new MockLoginStoringRestRequester();
            SFSession sfSession = new SFSession($"account=test;user=test;password=test{passcode};passcodeInPassword=true;", null, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests[0];
            Assert.IsNull(loginRequest.data.passcode);
            Assert.AreEqual("passcode", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestPushWhenNoPasscodeAndPasscodeInPasswordIsFalse()
        {
            // arrange
            MockLoginStoringRestRequester restRequester = new MockLoginStoringRestRequester();
            SFSession sfSession = new SFSession($"account=test;user=test;password=test;passcodeInPassword=false;", null, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests[0];
            Assert.IsNull(loginRequest.data.passcode);
            Assert.AreEqual("push", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestPushAsDefaultSecondaryAuthentication()
        {
            // arrange
            MockLoginStoringRestRequester restRequester = new MockLoginStoringRestRequester();
            SFSession sfSession = new SFSession($"account=test;user=test;password=test", null, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests[0];
            Assert.IsNull(loginRequest.data.passcode);
            Assert.AreEqual("push", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestPushMFAWithAuthenticationCacheMFAToken()
        {
            // arrange
            var restRequester = new MockLoginMFATokenCacheRestRequester();
            var sfSession = new SFSession($"account=test;user=test;password=test;authenticator=username_password_mfa", null, restRequester);

            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests.Dequeue();
            Assert.IsNull(loginRequest.data.passcode);
            Assert.IsTrue(loginRequest.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value) && (bool)value);
            Assert.AreEqual("push", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestMFATokenCacheReturnedToSession()
        {
            // arrange
            var testToken = "testToken1234";
            var restRequester = new MockLoginMFATokenCacheRestRequester();
            var sfSession = new SFSession($"account=test;user=test;password=test;authenticator=username_password_mfa", null, restRequester);
            restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                mfaToken = testToken,
                authResponseSessionInfo = new SessionInfo()
            });
            // act
            sfSession.Open();

            // assert
            Assert.AreEqual(1, restRequester.LoginRequests.Count);
            var loginRequest = restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(SecureStringHelper.Decode(sfSession._mfaToken), testToken);
            Assert.IsNull(loginRequest.data.passcode);
            Assert.IsTrue(loginRequest.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value) && (bool)value);
            Assert.AreEqual("push", loginRequest.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestMFATokenCacheUsedInNewConnection()
        {
            // arrange
            var testToken = "testToken1234";
            var restRequester = new MockLoginMFATokenCacheRestRequester();
            var connectionString = $"account=test;user=test;password=test;authenticator=username_password_mfa";
            var sfSession = new SFSession(connectionString, null, restRequester);
            restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                mfaToken = testToken,
                authResponseSessionInfo = new SessionInfo()
            });
            sfSession.Open();
            var sfSessionWithCachedToken = new SFSession(connectionString, null, restRequester);
            // act
            sfSessionWithCachedToken.Open();

            // assert
            Assert.AreEqual(2, restRequester.LoginRequests.Count);
            var firstLoginRequest = restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(SecureStringHelper.Decode(sfSession._mfaToken), testToken);
            Assert.IsNull(firstLoginRequest.data.passcode);
            Assert.IsTrue(firstLoginRequest.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value) && (bool)value);
            Assert.AreEqual("push", firstLoginRequest.data.extAuthnDuoMethod);

            var secondLoginRequest = restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(secondLoginRequest.data.Token, testToken);
        }
    }
}
