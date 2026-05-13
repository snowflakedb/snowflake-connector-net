using Snowflake.Data.Tests.IntegrationTests;
using Xunit;

namespace Snowflake.Data.Tests;

[CollectionDefinition(SequentialIntegrationCollectionName, DisableParallelization = true)]
public class SequentialIntegrationCollection : ICollectionFixture<SequentialIntegrationFixture>
{
    public const string SequentialIntegrationCollectionName = "SequentialIntegration";
}

public class SequentialIntegrationFixture : IntegrationTestFixture
{
}
