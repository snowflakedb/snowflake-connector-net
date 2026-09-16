using System;
using System.IO;
using System.Text;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class EncryptionProviderStreamSpillTest
{
    private const string PlainText = "test content for encryption stream spill verification.";
    private static readonly byte[] s_plainTextBytes = Encoding.UTF8.GetBytes(PlainText);

    private static readonly PutGetEncryptionMaterial s_encryptionMaterial = new()
    {
        queryStageMasterKey = Convert.ToBase64String(new byte[16]),
        queryId = "test-query-id",
        smkId = 123
    };

    [SFFact]
    public void TestCbcEncryptStreamWithUnlimitedMemoryUsesMemoryStream()
    {
        // arrange
        var config = new FileTransferConfiguration
        {
            TempDir = null, // no temp dir — would fail if FileBackedOutputStream tried to spill
            MaxBytesInMemory = -1
        };
        var encryptionMetadata = new SFEncryptionMetadata();

        // act
        using var result = EncryptionProvider.EncryptStream(
            new MemoryStream(s_plainTextBytes),
            s_encryptionMaterial,
            encryptionMetadata,
            config);

        // assert
        Assert.IsType<MemoryStream>(result.MainStream);
        Assert.NotNull(encryptionMetadata.iv);
        Assert.NotNull(encryptionMetadata.key);
    }

    [SFFact]
    public void TestCbcEncryptStreamWithBoundedMemoryUsesFileBackedOutputStream()
    {
        // arrange
        var config = new FileTransferConfiguration
        {
            TempDir = Path.GetTempPath(),
            MaxBytesInMemory = 1024
        };
        var encryptionMetadata = new SFEncryptionMetadata();

        // act
        using var result = EncryptionProvider.EncryptStream(
            new MemoryStream(s_plainTextBytes),
            s_encryptionMaterial,
            encryptionMetadata,
            config);

        // assert
        Assert.IsType<FileBackedOutputStream>(result.MainStream);
    }

    [SFFact]
    public void TestCbcEncryptDecryptRoundTripWithUnlimitedMemory()
    {
        // arrange
        var config = new FileTransferConfiguration
        {
            TempDir = Path.GetTempPath(),
            MaxBytesInMemory = -1
        };
        var encryptionMetadata = new SFEncryptionMetadata();

        // act — encrypt
        byte[] encrypted;
        using (var result = EncryptionProvider.EncryptStream(
                   new MemoryStream(s_plainTextBytes),
                   s_encryptionMaterial,
                   encryptionMetadata,
                   config))
        {
            result.MainStream.Position = 0;
            var ms = new MemoryStream();
            result.MainStream.CopyTo(ms);
            encrypted = ms.ToArray();
        }

        // act — decrypt
        var tmpFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        try
        {
            File.WriteAllBytes(tmpFile, encrypted);
            var decryptedPath = EncryptionProvider.DecryptFile(
                tmpFile,
                s_encryptionMaterial,
                encryptionMetadata,
                config);

            // assert
            var decrypted = File.ReadAllText(decryptedPath);
            Assert.Equal(PlainText, decrypted);
            File.Delete(decryptedPath);
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [SFFact]
    public void TestGcmEncryptStreamWithUnlimitedMemoryUsesMemoryStream()
    {
        // arrange
        var keyBytes = new byte[16];
        new Random(42).NextBytes(keyBytes);
        var material = new PutGetEncryptionMaterial
        {
            queryStageMasterKey = Convert.ToBase64String(keyBytes),
            queryId = "gcm-test-query",
            smkId = 456
        };
        var config = new FileTransferConfiguration
        {
            TempDir = null, // no temp dir — would fail if FileBackedOutputStream tried to spill
            MaxBytesInMemory = -1
        };
        var encryptionMetadata = new SFEncryptionMetadata();

        // act
        using var result = GcmEncryptionProvider.Encrypt(
            material,
            encryptionMetadata,
            config,
            new MemoryStream(s_plainTextBytes),
            null,
            null);

        // assert
        Assert.IsType<MemoryStream>(result);
        Assert.NotNull(encryptionMetadata.iv);
        Assert.NotNull(encryptionMetadata.key);
    }

    [SFFact]
    public void TestGcmEncryptStreamWithBoundedMemoryUsesFileBackedOutputStream()
    {
        // arrange
        var keyBytes = new byte[16];
        new Random(42).NextBytes(keyBytes);
        var material = new PutGetEncryptionMaterial
        {
            queryStageMasterKey = Convert.ToBase64String(keyBytes),
            queryId = "gcm-test-query",
            smkId = 456
        };
        var config = new FileTransferConfiguration
        {
            TempDir = Path.GetTempPath(),
            MaxBytesInMemory = 1024
        };
        var encryptionMetadata = new SFEncryptionMetadata();

        // act
        using var result = GcmEncryptionProvider.Encrypt(
            material,
            encryptionMetadata,
            config,
            new MemoryStream(s_plainTextBytes),
            null,
            null);

        // assert
        Assert.IsType<FileBackedOutputStream>(result);
    }

    [SFFact]
    public void TestGcmEncryptDecryptRoundTripWithUnlimitedMemory()
    {
        // arrange
        var keyBytes = new byte[16];
        new Random(42).NextBytes(keyBytes);
        var material = new PutGetEncryptionMaterial
        {
            queryStageMasterKey = Convert.ToBase64String(keyBytes),
            queryId = "gcm-test-query",
            smkId = 456
        };
        var config = new FileTransferConfiguration
        {
            TempDir = null,
            MaxBytesInMemory = -1
        };
        var encryptionMetadata = new SFEncryptionMetadata();

        // act — encrypt
        byte[] encryptedContent;
        using (var encryptedStream = GcmEncryptionProvider.Encrypt(
                   material,
                   encryptionMetadata,
                   config,
                   new MemoryStream(s_plainTextBytes),
                   null,
                   null))
        {
            Assert.IsType<MemoryStream>(encryptedStream);
            encryptedStream.Position = 0;
            var ms = new MemoryStream();
            encryptedStream.CopyTo(ms);
            encryptedContent = ms.ToArray();
        }

        // act — decrypt
        using var decryptedStream = GcmEncryptionProvider.Decrypt(
            new MemoryStream(encryptedContent),
            material,
            encryptionMetadata,
            config);

        // assert
        Assert.IsType<MemoryStream>(decryptedStream);
        decryptedStream.Position = 0;
        var decryptedText = new StreamReader(decryptedStream).ReadToEnd();
        Assert.Equal(PlainText, decryptedText);
    }

    [SFTheory]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(1024)]
    public void TestFileTransferConfigurationFromFileMetadataRespectsMaxBytesInMemory(int maxBytes)
    {
        // arrange
        var metadata = new SFFileMetadata
        {
            tmpDir = "/some/dir",
            MaxBytesInMemory = maxBytes
        };

        // act
        var config = FileTransferConfiguration.FromFileMetadata(metadata);

        // assert
        Assert.Equal(maxBytes, config.MaxBytesInMemory);
        Assert.Equal("/some/dir", config.TempDir);
    }

    [SFFact]
    public void TestFileTransferConfigurationFallsBackToDefaultTempDir()
    {
        // arrange
        var metadata = new SFFileMetadata
        {
            tmpDir = null,
            MaxBytesInMemory = 1024
        };

        // act
        var config = FileTransferConfiguration.FromFileMetadata(metadata);

        // assert
        Assert.Equal(Path.GetTempPath(), config.TempDir);
    }

    [SFTheory]
    [InlineData(-1)]
    [InlineData(1)]
    [InlineData(1048576)]
    public void TestValidFileTransferMemoryThresholdValues(int threshold)
    {
        // arrange
        var connectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;FILE_TRANSFER_MEMORY_THRESHOLD={threshold};";

        // act
        var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

        // assert — no exception means valid
        Assert.Equal(threshold.ToString(), properties[SFSessionProperty.FILE_TRANSFER_MEMORY_THRESHOLD]);
    }
}
