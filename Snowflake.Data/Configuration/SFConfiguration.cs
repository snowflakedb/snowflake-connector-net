using System;

namespace Snowflake.Data.Configuration;

public class SFConfiguration
{
    public int ChunkDownloaderVersion;
    public int ChunkParserVersion;

    private SFConfiguration()
    {
        ChunkDownloaderVersion = 3;
        ChunkParserVersion = 3;
    }

    private static readonly SFConfiguration Config = new();

    public static SFConfiguration Instance()
    {
        return Config;
    }

    public int GetChunkParserVersion()
    {
        return ChunkParserVersion;
    }

    public int GetChunkDownloaderVersion()
    {
        return ChunkDownloaderVersion;
    }
}
