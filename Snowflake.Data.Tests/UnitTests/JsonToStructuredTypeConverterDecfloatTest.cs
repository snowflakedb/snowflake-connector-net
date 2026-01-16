using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Converter;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class JsonToStructuredTypeConverterDecfloatTest
    {
        [Test]
        public void TestConvertObjectWithDecfloatField()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "DecfloatValue", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"DecfloatValue\": \"123.456\"}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertObject<DecfloatTestObject>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(123.456m, result.DecfloatValue);
        }

        [Test]
        public void TestConvertObjectWithDecfloatFieldScientificNotation()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "DecfloatValue", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"DecfloatValue\": \"1.5e10\"}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertObject<DecfloatTestObject>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(15000000000m, result.DecfloatValue);
        }

        [Test]
        public void TestConvertObjectWithDecfloatFieldNegativeExponent()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "DecfloatValue", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"DecfloatValue\": \"1.5e-3\"}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertObject<DecfloatTestObject>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0.0015m, result.DecfloatValue);
        }

        [Test]
        public void TestConvertObjectWithNullableDecfloatField()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "NullableDecfloatValue", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"NullableDecfloatValue\": \"999.99\"}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertObject<NullableDecfloatTestObject>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(999.99m, result.NullableDecfloatValue);
        }

        [Test]
        public void TestConvertObjectWithNullDecfloatField()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "NullableDecfloatValue", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"NullableDecfloatValue\": null}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertObject<NullableDecfloatTestObject>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.IsNull(result.NullableDecfloatValue);
        }

        [Test]
        public void TestConvertArrayWithDecfloatElements()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { type = "DECFLOAT" }
            };
            var json = JArray.Parse("[\"1.1\", \"2.2\", \"3.3\"]");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertArray<decimal>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Length);
            Assert.AreEqual(1.1m, result[0]);
            Assert.AreEqual(2.2m, result[1]);
            Assert.AreEqual(3.3m, result[2]);
        }

        [Test]
        public void TestConvertMapWithDecfloatValues()
        {
            // Arrange
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "key", type = "TEXT" },
                new FieldMetadata { name = "value", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"a\": \"1.1\", \"b\": \"2.2\"}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertMap<string, decimal>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(1.1m, result["a"]);
            Assert.AreEqual(2.2m, result["b"]);
        }

        [Test]
        public void TestConvertObjectWithDecfloatToDouble()
        {
            // Arrange - test that DECFLOAT can be read into a double field
            var fields = new List<FieldMetadata>
            {
                new FieldMetadata { name = "DoubleValue", type = "DECFLOAT" }
            };
            var json = JObject.Parse("{\"DoubleValue\": \"123.456\"}");

            // Act
            var result = JsonToStructuredTypeConverter.ConvertObject<DoubleTestObject>(fields, json);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(123.456d, result.DoubleValue, 0.0001);
        }

        // Test classes for structured type conversion
        public class DecfloatTestObject
        {
            public decimal DecfloatValue { get; set; }
        }

        public class NullableDecfloatTestObject
        {
            public decimal? NullableDecfloatValue { get; set; }
        }

        public class DoubleTestObject
        {
            public double DoubleValue { get; set; }
        }
    }
}
