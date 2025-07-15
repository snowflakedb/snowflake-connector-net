using System;
using NUnit.Framework;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Configuration
{
    [TestFixture]
    public class ClientFeatureFlagsTest
    {
        [Test]
        [TestCase(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName, "true", true)]
        [TestCase(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName, "TRUE", true)]
        [TestCase(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName, "false", false)]
        [TestCase(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName, "", false)]
        [TestCase(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName, null, false)]
        [TestCase(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName, "not a bool value", false)]
        [TestCase("OTHER_VARIABLE_NAME", "true", false)]
        public void TestEnabledExperimentalAuthentication(string variableName, string variableValue, bool expectedValue)
        {
            // arrange
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable(variableName))
                .Returns(variableValue);

            // act
            var clientFeatures = new ClientFeatureFlags(environmentOperations.Object);

            // assert
            Assert.AreEqual(expectedValue, clientFeatures.IsEnabledExperimentalAuthentication);
        }

        [Test]
        public void TestDisabledExperimentalAuthenticationWhenCouldNotReadEnvVariable()
        {
            // arrange
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName))
                .Throws(() => new Exception("Could not read environmental variable"));

            // act
            var clientFeatures = new ClientFeatureFlags(environmentOperations.Object);

            // assert
            Assert.IsFalse(clientFeatures.IsEnabledExperimentalAuthentication);
        }

        [Test]
        public void TestFailForDisabledAuthentication()
        {
            // arrange
            var environmentOperations = new Mock<EnvironmentOperations>();
            environmentOperations
                .Setup(e => e.GetEnvironmentVariable(ClientFeatureFlags.EnabledExperimentalAuthenticationVariableName))
                .Returns("false");
            var clientFeatures = new ClientFeatureFlags(environmentOperations.Object);

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => clientFeatures.VerifyIfExperimentalAuthenticationEnabled("workload_identity"));

            // assert
            Assert.AreEqual(SFError.EXPERIMENTAL_AUTHENTICATION_DISABLED.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.That(thrown.Message, Does.Contain("Experimental authentication of 'workload_identity' is disabled. You can enable it by SF_ENABLE_EXPERIMENTAL_AUTHENTICATION environmental variable."));
        }
    }
}
