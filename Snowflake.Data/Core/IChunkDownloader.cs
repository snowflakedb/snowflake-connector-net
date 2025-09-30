using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    interface IChunkDownloader
    {
        Task<BaseResultChunk> GetNextChunkAsync();
    }
}
