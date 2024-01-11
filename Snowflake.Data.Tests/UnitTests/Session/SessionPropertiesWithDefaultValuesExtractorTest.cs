using System;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SessionPropertiesWithDefaultValuesExtractorTest
    {
        [Test]
        public void TestReturnExtractedValue()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15", null);
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            
            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true
            );
            
            // assert
            Assert.AreEqual(15, value);
        }

        [Test]
        public void TestReturnDefaultValueWhenValueIsMissing(
            [Values] bool failOnWrongValue)
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString($"account=test;user=test;password=test", null);
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().defaultValue);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true
            );
            
            // assert
            Assert.AreEqual(defaultValue, value);
        }

        [Test]
        public void TestReturnDefaultValueWhenPreValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15", null);
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().defaultValue);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => false,
                i => true
            );

            // assert
            Assert.AreEqual(defaultValue, value);
        }
        
        [Test]
        public void TestReturnDefaultValueWhenPostValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15", null);
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().defaultValue);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => i == defaultValue
            );

            // assert
            Assert.AreEqual(defaultValue, value);
        }

        [Test]
        public void TestReturnDefaultValueWhenExtractFails()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15X", null);
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, false);
            var defaultValue = int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().defaultValue);

            // act
            var value = extractor.ExtractPropertyWithDefaultValue(
                SFSessionProperty.CONNECTION_TIMEOUT,
                int.Parse,
                s => true,
                i => true
            );

            // assert
            Assert.AreEqual(defaultValue, value);
        }
        
        [Test]
        public void TestFailWhenPreValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15", null);
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
        
        [Test]
        public void TestFailWhenPostValidationFails()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15", null);
            var extractor = new SessionPropertiesWithDefaultValuesExtractor(properties, true);
            var defaultValue = int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().defaultValue);

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
        
        [Test]
        public void TestFailWhenExtractFails()
        {
            // arrange
            var properties = SFSessionProperties.parseConnectionString("account=test;user=test;password=test;connection_timeout=15X", null);
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

    }
}
