using System.Threading.Tasks;

namespace Snowflake.Data.Core;

internal interface IChunkDownloader
{
    Task<BaseResultChunk> GetNextChunkAsync();
}
