using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core;

internal interface IChunkParser
{
    /// <summary>
    /// Parse source data stream, result will be store into SFResultChunk.rowset
    /// </summary>
    /// <param name="chunk">The result chunk whose row data will be populated from the stream.</param>
    /// <param name="cancellationToken">Cancellation support.</param>
    Task ParseChunkAsync(IResultChunk chunk, CancellationToken cancellationToken);
}
