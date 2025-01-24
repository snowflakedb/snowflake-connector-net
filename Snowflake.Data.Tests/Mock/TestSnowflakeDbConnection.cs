using System.Data.Common;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.Mock
{
    public class TestSnowflakeDbConnection : SnowflakeDbConnection
    {
        public TestSnowflakeDbConnection(DbProviderFactory dbProviderFactory)
        {
            DbProviderFactory = dbProviderFactory;
        }

        protected override DbProviderFactory DbProviderFactory { get; }
    }
}
