using System;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.IntegrationTests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class SnowflakeDbConnectionPoolTest : IDisposable
    {
        private readonly string _connectionString1 = "database=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;";
        private readonly string _connectionString2 = "database=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;";

        public SnowflakeDbConnectionPoolTest()
        {
            ConnectionManagerTestsFacade.Init();
            ConnectionManagerTestsFacade.RegisterDedicatedContext(nameof(SnowflakeDbConnectionPoolTest), ConnectionPoolType.SingleConnectionCache);
        }

        [SFFact]
        public void TestRevertPoolToPreviousVersion()
        {
            // act
            SnowflakeDbConnectionPool.SetOldConnectionPoolVersion();

            // assert
            var sessionPool1 = SnowflakeDbConnectionPool.GetPoolInternal(_connectionString1);
            var sessionPool2 = SnowflakeDbConnectionPool.GetPoolInternal(_connectionString2);
            Assert.Equal(ConnectionPoolType.SingleConnectionCache, SnowflakeDbConnectionPool.GetConnectionPoolVersion());
            Assert.Equal(sessionPool1, sessionPool2);
        }

        public void Dispose()
        {
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(nameof(SnowflakeDbConnectionPoolTest));
        }
    }
}
