using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    interface IChunkParser
    {
        /// <summary>
        ///     Parse source data stream, result will be store into SFResultChunk.rowset
        /// </summary>
        /// <param name="chunk"></param>
        /// <param name="cancellationToken"></param>
        Task ParseChunkAsync(IResultChunk chunk, CancellationToken cancellationToken);
    }
}
