using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class ConnectionPoolConfigExtractorTest
    {
        [Test]
        public void TestExtractDefaultValues()
        {
            // arrange
            var connectionString = "account=test;user=test;password=test;";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultMaxPoolSize, result.MaxPoolSize, "max pool size");
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultMinPoolSize, result.MinPoolSize, "min pool size");
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultChangedSession, result.ChangedSession, "changed session");
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultExpirationTimeout, result.ExpirationTimeout, "expiration timeout");
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultWaitingForIdleSessionTimeout, result.WaitingForIdleSessionTimeout, "waiting for idle session timeout");
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultConnectionTimeout, result.ConnectionTimeout, "connection timeout");
            Assert.AreEqual(SFSessionHttpClientProperties.DefaultPoolingEnabled, result.PoolingEnabled, "pooling enabled");
        }

        [Test]
        public void TestExtractMaxPoolSize()
        {
            // arrange
            var maxPoolSize = 15;
            var connectionString = $"account=test;user=test;password=test;maxPoolSize={maxPoolSize}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(maxPoolSize, result.MaxPoolSize);
        }

        [Test]
        [TestCase("wrong_value")]
        [TestCase("0")]
        [TestCase("-1")]
        public void TestExtractFailsForWrongValueOfMaxPoolSize(string maxPoolSize)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;maxPoolSize={maxPoolSize}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain($"Invalid value of parameter {SFSessionProperty.MAXPOOLSIZE.ToString()}"));
        }

        [Test]
        [TestCase("0", 0)]
        [TestCase("7", 7)]
        [TestCase("10", 10)]
        public void TestExtractMinPoolSize(string propertyValue, int expectedMinPoolSize)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;minPoolSize={propertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(expectedMinPoolSize, result.MinPoolSize);
        }

        [Test]
        [TestCase("wrong_value")]
        [TestCase("-1")]
        public void TestExtractFailsForWrongValueOfMinPoolSize(string minPoolSize)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;minPoolSize={minPoolSize}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain($"Invalid value of parameter {SFSessionProperty.MINPOOLSIZE.ToString()}"));
        }

        [Test]
        public void TestExtractFailsWhenMinPoolSizeGreaterThanMaxPoolSize()
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;minPoolSize=10;maxPoolSize=9";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain("MinPoolSize cannot be greater than MaxPoolSize"));
        }

        [Test]
        [TestCaseSource(nameof(CorrectTimeoutsWithZeroUnchanged))]
        public void TestExtractExpirationTimeout(TimeoutTestCase testCase)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;expirationTimeout={testCase.PropertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(testCase.ExpectedTimeout, result.ExpirationTimeout);
        }

        [Test]
        [TestCaseSource(nameof(IncorrectTimeouts))]
        public void TestExtractExpirationTimeoutFailsWhenWrongValue(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;expirationTimeout={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain($"Invalid value of parameter {SFSessionProperty.EXPIRATIONTIMEOUT.ToString()}"));
        }

        [Test]
        [TestCaseSource(nameof(PositiveTimeoutsAndZeroUnchanged))]
        public void TestExtractWaitingForIdleSessionTimeout(TimeoutTestCase testCase)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;waitingForIdleSessionTimeout={testCase.PropertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(testCase.ExpectedTimeout, result.WaitingForIdleSessionTimeout);
        }

        [Test]
        public void TestExtractWaitingForIdleSessionTimeoutFailsForInfiniteTimeout()
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;waitingForIdleSessionTimeout=-1";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain("Waiting for idle session timeout cannot be infinite"));
        }

        [Test]
        [TestCaseSource(nameof(IncorrectTimeouts))]
        public void TestExtractWaitingForIdleSessionTimeoutFailsWhenWrongValue(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;waitingForIdleSessionTimeout={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain($"Invalid value of parameter {SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT.ToString()}"));
        }

        [Test]
        [TestCaseSource(nameof(CorrectTimeoutsWithZeroAsInfinite))]
        public void TestExtractConnectionTimeout(TimeoutTestCase testCase)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;CONNECTION_TIMEOUT={testCase.PropertyValue};RETRY_TIMEOUT=60m";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(testCase.ExpectedTimeout, result.ConnectionTimeout);
        }

        [Test]
        [TestCaseSource(nameof(IncorrectTimeouts))]
        public void TestExtractConnectionTimeoutFailsForWrongValue(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;CONNECTION_TIMEOUT={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain($"Invalid value of parameter {SFSessionProperty.CONNECTION_TIMEOUT.ToString()}"));
        }

        [Test]
        [TestCase("true", true)]
        [TestCase("TRUE", true)]
        [TestCase("false", false)]
        [TestCase("FALSE", false)]
        public void TestExtractPoolingEnabled(string propertyValue, bool poolingEnabled)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;poolingEnabled={propertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(poolingEnabled, result.PoolingEnabled);
        }

        [Test]
        [TestCase("account=test;user=test;password=test;", true)]
        [TestCase("authenticator=externalbrowser;account=test;user=test;", false)]
        [TestCase("authenticator=externalbrowser;account=test;user=test;poolingEnabled=true;", true)]
        [TestCase("authenticator=externalbrowser;account=test;user=test;poolingEnabled=false;", false)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key", false)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;poolingEnabled=true;", true)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;poolingEnabled=false;", false)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key=secretKey", true)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key=secretKey;poolingEnabled=true;", true)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key=secretKey;poolingEnabled=false;", false)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;private_key_pwd=secretPwd", true)]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;private_key_pwd=", false)]
        public void TestDisablePoolingDefaultWhenSecretsProvidedExternally(string connectionString, bool poolingEnabled)
        {
            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(poolingEnabled, result.PoolingEnabled);
        }

        [Test]
        [TestCase("wrong_value")]
        [TestCase("15")]
        public void TestExtractFailsForWrongValueOfPoolingEnabled(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;poolingEnabled={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.That(thrown.Message, Does.Contain($"Invalid value of parameter {SFSessionProperty.POOLINGENABLED.ToString()}"));
        }

        [Test]
        [TestCase("OriginalPool", ChangedSessionBehavior.OriginalPool)]
        [TestCase("originalpool", ChangedSessionBehavior.OriginalPool)]
        [TestCase("ORIGINALPOOL", ChangedSessionBehavior.OriginalPool)]
        [TestCase("Destroy", ChangedSessionBehavior.Destroy)]
        [TestCase("DESTROY", ChangedSessionBehavior.Destroy)]
        public void TestExtractChangedSessionBehaviour(string propertyValue, ChangedSessionBehavior expectedChangedSession)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;changedSession={propertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.AreEqual(expectedChangedSession, result.ChangedSession);
        }

        private ConnectionPoolConfig ExtractConnectionPoolConfig(string connectionString) =>
            SessionPool.ExtractConfig(connectionString, new SessionPropertiesContext()).Item1;

        public class TimeoutTestCase
        {
            public string PropertyValue { get; }
            public TimeSpan ExpectedTimeout { get; }

            public TimeoutTestCase(string propertyValue, TimeSpan expectedTimeout)
            {
                PropertyValue = propertyValue;
                ExpectedTimeout = expectedTimeout;
            }
        }

        public static IEnumerable<TimeoutTestCase> CorrectTimeoutsWithZeroUnchanged() =>
            CorrectTimeoutsWithoutZero().Concat(ZeroUnchangedTimeouts());

        public static IEnumerable<TimeoutTestCase> CorrectTimeoutsWithZeroAsInfinite() =>
            CorrectTimeoutsWithoutZero().Concat(ZeroAsInfiniteTimeouts());

        public static IEnumerable<TimeoutTestCase> PositiveTimeoutsAndZeroUnchanged() =>
            PositiveTimeouts().Concat(ZeroUnchangedTimeouts());

        private static IEnumerable<TimeoutTestCase> CorrectTimeoutsWithoutZero() =>
            NegativeAsInfinityTimeouts().Concat(PositiveTimeouts());

        private static IEnumerable<TimeoutTestCase> NegativeAsInfinityTimeouts()
        {
            yield return new TimeoutTestCase("-1", TimeoutHelper.Infinity());
        }

        private static IEnumerable<TimeoutTestCase> PositiveTimeouts()
        {
            yield return new TimeoutTestCase("5", TimeSpan.FromSeconds(5));
            yield return new TimeoutTestCase("6s", TimeSpan.FromSeconds(6));
            yield return new TimeoutTestCase("7S", TimeSpan.FromSeconds(7));
            yield return new TimeoutTestCase("8m", TimeSpan.FromMinutes(8));
            yield return new TimeoutTestCase("9M", TimeSpan.FromMinutes(9));
            yield return new TimeoutTestCase("10ms", TimeSpan.FromMilliseconds(10));
            yield return new TimeoutTestCase("11ms", TimeSpan.FromMilliseconds(11));
        }

        private static IEnumerable<TimeoutTestCase> ZeroAsInfiniteTimeouts()
        {
            yield return new TimeoutTestCase("0", TimeoutHelper.Infinity());
            yield return new TimeoutTestCase("0ms", TimeoutHelper.Infinity());
        }

        private static IEnumerable<TimeoutTestCase> ZeroUnchangedTimeouts()
        {
            yield return new TimeoutTestCase("0", TimeSpan.Zero);
            yield return new TimeoutTestCase("0ms", TimeSpan.Zero);
        }

        public static IEnumerable<string> IncorrectTimeouts()
        {
            yield return "wrong value";
            yield return "1h";
            yield return "1s1s";
        }
    }
}
