using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    public class SnowflakeDbConnectionPoolTest
    {
        private readonly string _connectionString1 = "database=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;";
        private readonly string _connectionString2 = "database=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;";

        [Test]
        public void TestRevertPoolToPreviousVersion()
        {
            // act
            SnowflakeDbConnectionPool.SetOldConnectionPoolVersion();

            // assert
            var sessionPool1 = SnowflakeDbConnectionPool.GetPoolInternal(_connectionString1);
            var sessionPool2 = SnowflakeDbConnectionPool.GetPoolInternal(_connectionString2);
            Assert.AreEqual(ConnectionPoolType.SingleConnectionCache, SnowflakeDbConnectionPool.GetConnectionPoolVersion());
            Assert.AreEqual(sessionPool1, sessionPool2);
        }
    }
}
