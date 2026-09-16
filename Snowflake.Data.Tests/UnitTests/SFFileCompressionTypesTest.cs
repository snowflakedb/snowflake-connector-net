using System.IO;
using System.IO.Compression;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class SFFileCompressionTypesTest
{
    [SFFact]
    public void TestGuessCompressionType_EmptyStream_ReturnsNone()
    {
        var stream = new MemoryStream([]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal(SFFileCompressionTypes.NONE, result);
    }

    [SFTheory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void TestGuessCompressionType_StreamShorterThanMagicBytes_ReturnsNone(int length)
    {
        var stream = new MemoryStream(new byte[length]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal(SFFileCompressionTypes.NONE, result);
    }

    [SFFact]
    public void TestGuessCompressionType_GzipMagic_ReturnsGzip()
    {
        var stream = new MemoryStream([0x1f, 0x8b, 0x00, 0x00]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("gzip", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_RealGzipStream_ReturnsGzip()
    {
        using var compressed = new MemoryStream();
        using (var gzip = new GZipStream(compressed, CompressionMode.Compress, leaveOpen: true))
            gzip.Write("hello world"u8);
        compressed.Position = 0;

        var result = SFFileCompressionTypes.GuessCompressionType(compressed);
        Assert.Equal("gzip", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_DeflateMagicLow_ReturnsDeflate()
    {
        var stream = new MemoryStream([0x78, 0x01, 0x00, 0x00]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("deflate", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_DeflateMagicDefault_ReturnsDeflate()
    {
        var stream = new MemoryStream([0x78, 0x9c, 0x00, 0x00]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("deflate", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_DeflateMagicBest_ReturnsDeflate()
    {
        var stream = new MemoryStream([0x78, 0xda, 0x00, 0x00]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("deflate", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_Bzip2Magic_ReturnsBzip2()
    {
        var stream = new MemoryStream([0x42, 0x5a, 0x00, 0x00]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("bzip2", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_ZstdMagic_ReturnsZstd()
    {
        var stream = new MemoryStream([0x28, 0xb5, 0x2f, 0xfd]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("zstd", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_BrotliMagic_ReturnsBrotli()
    {
        var stream = new MemoryStream([0xce, 0xb2, 0xcf, 0x81]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("brotli", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_OrcMagic_ReturnsOrc()
    {
        var stream = new MemoryStream([0x4f, 0x52, 0x43, 0x00]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("orc", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_ParquetMagic_ReturnsParquet()
    {
        var stream = new MemoryStream([0x50, 0x41, 0x52, 0x31]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal("parquet", result.Name);
    }

    [SFFact]
    public void TestGuessCompressionType_UnknownMagic_ReturnsNone()
    {
        var stream = new MemoryStream([0xFF, 0xFE, 0xFD, 0xFC]);
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal(SFFileCompressionTypes.NONE, result);
    }

    [SFFact]
    public void TestGuessCompressionType_PlainCsvData_ReturnsNone()
    {
        var stream = new MemoryStream("col1,col2\n1,2\n"u8.ToArray());
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal(SFFileCompressionTypes.NONE, result);
    }

    [SFFact]
    public void TestGuessCompressionType_ResetsStreamPosition()
    {
        var data = new byte[] { 0x1f, 0x8b, 0x00, 0x00, 0x01, 0x02 };
        var stream = new MemoryStream(data);
        SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal(0, stream.Position);
    }

    [SFFact]
    public void TestGuessCompressionType_StreamAtNonZeroPosition_ReadsFromCurrentPosition()
    {
        // Stream positioned past gzip magic — should not detect gzip
        var data = new byte[] { 0x1f, 0x8b, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF };
        var stream = new MemoryStream(data) { Position = 4 };
        var result = SFFileCompressionTypes.GuessCompressionType(stream);
        Assert.Equal(SFFileCompressionTypes.NONE, result);
    }
}
