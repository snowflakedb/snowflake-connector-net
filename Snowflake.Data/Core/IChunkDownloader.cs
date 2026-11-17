using System.Threading.Tasks;

namespace Snowflake.Data.Core;

internal interface IChunkDownloader
{
    Task<BaseResultChunk> GetNextChunkAsync();

    /// <summary>
    /// Releases the data held by all chunks of the downloader, waiting for in-flight downloads to finish first.
    /// The downloader must not be used afterwards.
    /// </summary>
    void ReleaseChunks();
}
