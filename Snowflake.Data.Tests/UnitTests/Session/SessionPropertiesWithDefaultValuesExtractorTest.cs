using System;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    public class SessionPropertiesWithDefaultValuesExtractorTest
    {
        [SFFact]
        public void TestReturnExtractedValue()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true
            );

            // assert
            Assert.Equal(15, value);
        }

        [SFFact]
        public void TestReturnDefaultValueWhenValueIsMissing(
            [Values] bool failOnWrongValue)
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString($"account=test;user=test;password=test", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = GetDefaultIntSessionProperty(SFSessionProperty.CONNECTION_TIMEOUT);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true
            );

            // assert
            Assert.Equal(defaultValue, value);
        }

        [SFFact]
        public void TestReturnDefaultValueWhenPreValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = GetDefaultIntSessionProperty(SFSessionProperty.CONNECTION_TIMEOUT);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => false,
                i => true
            );

            // assert
            Assert.Equal(defaultValue, value);
        }

        [SFFact]
        public void TestFailForPropertyWithInvalidDefaultValue()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);

            // act
            var thrown = Assert.Throws<Exception>(() => extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                s => s,
                s => true,
                s => false));

            // assert
            Assert.That(thrown.Message, Does.Contain("Invalid default value of CONNECTION_TIMEOUT"));
        }

        [SFFact]
        public void TestReturnDefaultValueForNullProperty()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;", new SessionPropertiesContext());
            properties[SFSessionProperty.CONNECTION_TIMEOUT] = null;
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = GetDefaultIntSessionProperty(SFSessionProperty.CONNECTION_TIMEOUT);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true);

            // assert
            Assert.Equal(defaultValue, value);
        }

        [SFFact]
        public void TestReturnDefaultValueWhenPostValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = GetDefaultIntSessionProperty(SFSessionProperty.CONNECTION_TIMEOUT);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => i == defaultValue
            );

            // assert
            Assert.Equal(defaultValue, value);
        }

        [SFFact]
        public void TestReturnDefaultValueWhenExtractFails()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15X", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = GetDefaultIntSessionProperty(SFSessionProperty.CONNECTION_TIMEOUT);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true
            );

            // assert
            Assert.Equal(defaultValue, value);
        }

        [SFFact]
        public void TestFailWhenPreValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, true);

            // act
            var thrown = Assert.Throws<Exception>(() =>
                extractor.ExtractPropertyWithDefaultValue(
                    SFSessionProperty.CONNECTION_TIMEOUT,
                    int.Parse,
                    s => false,
                    i => true
                ));

            // assert
            Assert.That(thrown.Message, Does.Contain("Invalid value of parameter CONNECTION_TIMEOUT"));
        }

        [SFFact]
        public void TestFailWhenPostValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, true);
            var defaultValue = GetDefaultIntSessionProperty(SFSessionProperty.CONNECTION_TIMEOUT);

            // act
            var thrown = Assert.Throws<Exception>(() =>
                extractor.ExtractPropertyWithDefaultValue(
                    SFSessionProperty.CONNECTION_TIMEOUT,
                    int.Parse,
                    s => true,
                    i => i == defaultValue
                ));

            // assert
            Assert.That(thrown.Message, Does.Contain("Invalid value of parameter CONNECTION_TIMEOUT"));
        }

        [SFFact]
        public void TestFailWhenExtractFails()
        {
            // arrange
            var properties = SFSessionProperties.ParseConnectionString("account=test;user=test;password=test;connection_timeout=15X", new SessionPropertiesContext());
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, true);

            // act
            var thrown = Assert.Throws<Exception>(() =>
                extractor.ExtractPropertyWithDefaultValue(
                    SFSessionProperty.CONNECTION_TIMEOUT,
                    int.Parse,
                    s => true,
                    i => true
                ));

            // assert
            Assert.That(thrown.Message, Does.Contain("Invalid value of parameter CONNECTION_TIMEOUT"));
        }

        private int GetDefaultIntSessionProperty(SFSessionProperty property) =>
            int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().defaultValue);
    }
}
