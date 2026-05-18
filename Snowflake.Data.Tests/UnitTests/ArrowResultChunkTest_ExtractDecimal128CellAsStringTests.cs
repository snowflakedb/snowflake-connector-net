using System;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests;

[TestFixture]
public partial class ArrowResultChunkTest
{
    [TestFixture]
    public sealed class ExtractDecimal128CellAsStringTests
    {
        [TestCase("99999999999999999999999999999", 0, "99999999999999999999999999999")]
        [TestCase("-99999999999999999999999999999", 0, "-99999999999999999999999999999")]
        [TestCase("123456789012345678901234567890", 0, "123456789012345678901234567890")]
        [TestCase("-123456789012345678901234567890", 0, "-123456789012345678901234567890")]
        public void TestExtractDecimal128CellAsStringScaleZero(string rawValue, int scale, string expected)
        {
            var chunk = BuildDecimal128Chunk(BigInteger.Parse(rawValue), scale);
            chunk.Next();
            Assert.AreEqual(expected, chunk.ExtractDecimal128CellAsString(0, scale));
        }

        [TestCase("99999999999999999999999999999", 2, "999999999999999999999999999.99")]
        [TestCase("-99999999999999999999999999999", 2, "-999999999999999999999999999.99")]
        [TestCase("123456789012345678901234567890", 5, "1234567890123456789012345.67890")]
        [TestCase("-123456789012345678901234567890", 5, "-1234567890123456789012345.67890")]
        [TestCase("99999999999999999999999999999999999999", 10, "9999999999999999999999999999.9999999999")]
        public void TestExtractDecimal128CellAsStringWithScale(string rawValue, int scale, string expected)
        {
            var chunk = BuildDecimal128Chunk(BigInteger.Parse(rawValue), scale);
            chunk.Next();
            Assert.AreEqual(expected, chunk.ExtractDecimal128CellAsString(0, scale));
        }

        [TestCase("1", 5, "0.00001")]
        [TestCase("-1", 5, "-0.00001")]
        [TestCase("123", 5, "0.00123")]
        [TestCase("-123", 5, "-0.00123")]
        [TestCase("12345", 5, "0.12345")]
        [TestCase("-12345", 5, "-0.12345")]
        [TestCase("99999", 5, "0.99999")]
        public void TestExtractDecimal128CellAsStringFewerDigitsThanScale(string rawValue, int scale, string expected)
        {
            var chunk = BuildDecimal128Chunk(BigInteger.Parse(rawValue), scale);
            chunk.Next();
            Assert.AreEqual(expected, chunk.ExtractDecimal128CellAsString(0, scale));
        }

        [Test]
        public void TestExtractDecimal128CellAsStringZeroValues()
        {
            var chunk = BuildDecimal128Chunk(BigInteger.Zero, 0);
            chunk.Next();
            Assert.AreEqual("0", chunk.ExtractDecimal128CellAsString(0, 0));

            chunk = BuildDecimal128Chunk(BigInteger.Zero, 5);
            chunk.Next();
            Assert.AreEqual("0.00000", chunk.ExtractDecimal128CellAsString(0, 5));
        }

        [TestCase("79228162514264337593543950336", 0, "79228162514264337593543950336")]
        [TestCase("-79228162514264337593543950336", 0, "-79228162514264337593543950336")]
        [TestCase("79228162514264337593543950336", 2, "792281625142643375935439503.36")]
        [TestCase("-79228162514264337593543950336", 2, "-792281625142643375935439503.36")]
        public void TestExtractDecimal128CellAsStringAtDecimalMaxBoundary(string rawValue, int scale, string expected)
        {
            // decimal.MaxValue = 79228162514264337593543950335
            // These values are exactly 1 above decimal.MaxValue (unscaled), confirming the overflow path works
            var chunk = BuildDecimal128Chunk(BigInteger.Parse(rawValue), scale);
            chunk.Next();
            Assert.AreEqual(expected, chunk.ExtractDecimal128CellAsString(0, scale));
        }

        private static ArrowResultChunk BuildDecimal128Chunk(BigInteger value, int scale)
        {
            var bytes = value.ToByteArray();
            // Pad or truncate to exactly 16 bytes (little-endian two's complement)
            var padded = new byte[16];
            var signByte = (byte)(value.Sign < 0 ? 0xFF : 0x00);
            for (var i = 0; i < 16; i++)
                padded[i] = signByte;
            Buffer.BlockCopy(bytes, 0, padded, 0, Math.Min(bytes.Length, 16));

            var valueBuffer = new ArrowBuffer(padded);
            var nullBuffer = new ArrowBuffer(new byte[] { 0x01 }); // 1 valid bit
            var decimalType = new Decimal128Type(38, scale);
            var arrayData = new ArrayData(decimalType, 1, 0, 0, [nullBuffer, valueBuffer]);
            var column = new Decimal128Array(arrayData);

            var schema = new Schema([new Field("Col_Decimal128", decimalType, nullable: false)], null);
            var recordBatch = new RecordBatch(schema, [column], 1);
            return new ArrowResultChunk(recordBatch);
        }
    }
}
