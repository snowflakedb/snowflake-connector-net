using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    public class ConnectionPoolConfigExtractorTest
    {
        [Fact]
        public void TestExtractDefaultValues()
        {
            // arrange
            var connectionString = "account=test;user=test;password=test;";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(SFSessionHttpClientProperties.DefaultMaxPoolSize, result.MaxPoolSize);
            Assert.Equal(SFSessionHttpClientProperties.DefaultMinPoolSize, result.MinPoolSize);
            Assert.Equal(SFSessionHttpClientProperties.DefaultChangedSession, result.ChangedSession);
            Assert.Equal(SFSessionHttpClientProperties.DefaultExpirationTimeout, result.ExpirationTimeout);
            Assert.Equal(SFSessionHttpClientProperties.DefaultWaitingForIdleSessionTimeout, result.WaitingForIdleSessionTimeout);
            Assert.Equal(SFSessionHttpClientProperties.DefaultConnectionTimeout, result.ConnectionTimeout);
            Assert.Equal(SFSessionHttpClientProperties.DefaultPoolingEnabled, result.PoolingEnabled);
        }

        [Fact]
        public void TestExtractMaxPoolSize()
        {
            // arrange
            var maxPoolSize = 15;
            var connectionString = $"account=test;user=test;password=test;maxPoolSize={maxPoolSize}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(maxPoolSize, result.MaxPoolSize);
        }

        [Theory]
        [InlineData("wrong_value")]
        [InlineData("0")]
        [InlineData("-1")]
        public void TestExtractFailsForWrongValueOfMaxPoolSize(string maxPoolSize)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;maxPoolSize={maxPoolSize}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains($"Invalid value of parameter {SFSessionProperty.MAXPOOLSIZE.ToString()}", thrown.Message);
        }

        [Theory]
        [InlineData("0", 0)]
        [InlineData("7", 7)]
        [InlineData("10", 10)]
        public void TestExtractMinPoolSize(string propertyValue, int expectedMinPoolSize)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;minPoolSize={propertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(expectedMinPoolSize, result.MinPoolSize);
        }

        [Theory]
        [InlineData("wrong_value")]
        [InlineData("-1")]
        public void TestExtractFailsForWrongValueOfMinPoolSize(string minPoolSize)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;minPoolSize={minPoolSize}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains($"Invalid value of parameter {SFSessionProperty.MINPOOLSIZE.ToString()}", thrown.Message);
        }

        [Fact]
        public void TestExtractFailsWhenMinPoolSizeGreaterThanMaxPoolSize()
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;minPoolSize=10;maxPoolSize=9";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains("MinPoolSize cannot be greater than MaxPoolSize", thrown.Message);
        }

        [Theory]
        [MemberData(nameof(CorrectTimeoutsWithZeroUnchanged))]
        public void TestExtractExpirationTimeout(TimeoutTestCase testCase)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;expirationTimeout={testCase.PropertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(testCase.ExpectedTimeout, result.ExpirationTimeout);
        }

        [Theory]
        [MemberData(nameof(IncorrectTimeouts))]
        public void TestExtractExpirationTimeoutFailsWhenWrongValue(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;expirationTimeout={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains($"Invalid value of parameter {SFSessionProperty.EXPIRATIONTIMEOUT.ToString()}", thrown.Message);
        }

        [Theory]
        [MemberData(nameof(PositiveTimeoutsAndZeroUnchanged))]
        public void TestExtractWaitingForIdleSessionTimeout(TimeoutTestCase testCase)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;waitingForIdleSessionTimeout={testCase.PropertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(testCase.ExpectedTimeout, result.WaitingForIdleSessionTimeout);
        }

        [Fact]
        public void TestExtractWaitingForIdleSessionTimeoutFailsForInfiniteTimeout()
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;waitingForIdleSessionTimeout=-1";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains("Waiting for idle session timeout cannot be infinite", thrown.Message);
        }

        [Theory]
        [MemberData(nameof(IncorrectTimeouts))]
        public void TestExtractWaitingForIdleSessionTimeoutFailsWhenWrongValue(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;waitingForIdleSessionTimeout={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains($"Invalid value of parameter {SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT.ToString()}", thrown.Message);
        }

        [Theory]
        [MemberData(nameof(CorrectTimeoutsWithZeroAsInfinite))]
        public void TestExtractConnectionTimeout(TimeoutTestCase testCase)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;CONNECTION_TIMEOUT={testCase.PropertyValue};RETRY_TIMEOUT=60m";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(testCase.ExpectedTimeout, result.ConnectionTimeout);
        }

        [Theory]
        [MemberData(nameof(IncorrectTimeouts))]
        public void TestExtractConnectionTimeoutFailsForWrongValue(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;CONNECTION_TIMEOUT={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains($"Invalid value of parameter {SFSessionProperty.CONNECTION_TIMEOUT.ToString()}", thrown.Message);
        }

        [Theory]
        [InlineData("true", true)]
        [InlineData("TRUE", true)]
        [InlineData("false", false)]
        [InlineData("FALSE", false)]
        public void TestExtractPoolingEnabled(string propertyValue, bool poolingEnabled)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;poolingEnabled={propertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(poolingEnabled, result.PoolingEnabled);
        }

        [Theory]
        [InlineData("account=test;user=test;password=test;", true)]
        [InlineData("authenticator=externalbrowser;account=test;user=test;", false)]
        [InlineData("authenticator=externalbrowser;account=test;user=test;poolingEnabled=true;", true)]
        [InlineData("authenticator=externalbrowser;account=test;user=test;poolingEnabled=false;", false)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key", false)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;poolingEnabled=true;", true)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;poolingEnabled=false;", false)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key=secretKey", true)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key=secretKey;poolingEnabled=true;", true)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key=secretKey;poolingEnabled=false;", false)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;private_key_pwd=secretPwd", true)]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key;private_key_pwd=", false)]
        public void TestDisablePoolingDefaultWhenSecretsProvidedExternally(string connectionString, bool poolingEnabled)
        {
            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(poolingEnabled, result.PoolingEnabled);
        }

        [Theory]
        [InlineData("wrong_value")]
        [InlineData("15")]
        public void TestExtractFailsForWrongValueOfPoolingEnabled(string propertyValue)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;poolingEnabled={propertyValue}";

            // act
            var thrown = Assert.Throws<Exception>(() => ExtractConnectionPoolConfig(connectionString));

            // assert
            Assert.Contains($"Invalid value of parameter {SFSessionProperty.POOLINGENABLED.ToString()}", thrown.Message);
        }

        [Theory]
        [InlineData("OriginalPool", ChangedSessionBehavior.OriginalPool)]
        [InlineData("originalpool", ChangedSessionBehavior.OriginalPool)]
        [InlineData("ORIGINALPOOL", ChangedSessionBehavior.OriginalPool)]
        [InlineData("Destroy", ChangedSessionBehavior.Destroy)]
        [InlineData("DESTROY", ChangedSessionBehavior.Destroy)]
        public void TestExtractChangedSessionBehaviour(string propertyValue, ChangedSessionBehavior expectedChangedSession)
        {
            // arrange
            var connectionString = $"account=test;user=test;password=test;changedSession={propertyValue}";

            // act
            var result = ExtractConnectionPoolConfig(connectionString);

            // assert
            Assert.Equal(expectedChangedSession, result.ChangedSession);
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
