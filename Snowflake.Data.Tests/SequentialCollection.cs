using Xunit;

namespace Snowflake.Data.Tests;

[CollectionDefinition(SequentialCollectionName, DisableParallelization = true)]
public class SequentialCollection
{
    public const string SequentialCollectionName = "Sequential";
}
