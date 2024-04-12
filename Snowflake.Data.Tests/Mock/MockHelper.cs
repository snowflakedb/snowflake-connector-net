using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    public static class MockHelper
    {
        public static Mock<SnowflakeDbCommand> CommandThrowingExceptionOnlyForRollback()
        {
            var command = new Mock<SnowflakeDbCommand>();
            command.CallBase = true;
            command.SetupSet(it => it.CommandText = "ROLLBACK")
                .Throws(new SnowflakeDbException(SFError.INTERNAL_ERROR, "Unexpected failure on transaction rollback when connection is returned to the pool with pending transaction"));
            return command;
        }
    }
}
