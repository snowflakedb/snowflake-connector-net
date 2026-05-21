namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Client;
    using System;
    using System.Threading;
    using System.Threading.Tasks;


    class SFDbCommandTest
    {
        SnowflakeDbCommand command;

        [SetUp]
        public void BeforeTest()
        {
            command = new SnowflakeDbCommand();
        }

        [SFFact]
        public void TestCommandWithConnectionAndCommandText()
        {
            // Arrange
            SnowflakeDbConnection conn = new SnowflakeDbConnection();
            string commandText = "select 1";

            // Act
            command = new SnowflakeDbCommand(conn, commandText);

            // Assert
            Assert.Equal(conn, command.Connection);
            Assert.Equal(commandText, command.CommandText);
        }

        [SFFact]
        public void TestCommandExecuteThrowsExceptionWhenCommandTextIsNotSet()
        {
            // Act
            var thrown = Assert.Throws<Exception>(() => command.ExecuteScalar());

            // Assert
            Assert.Equal(thrown.Message, "Unable to execute command due to command text not being set");
        }

        [SFFact]
        public void TestCommandExecuteAsyncThrowsExceptionWhenCommandTextIsNotSet()
        {
            // Arrange
            Task<object> commandTask = command.ExecuteScalarAsync(CancellationToken.None);

            // Act
            var thrown = Assert.Throws<AggregateException>(() => commandTask.Wait());

            // Assert
            Assert.Equal(thrown.InnerException.Message, "Unable to execute command due to command text not being set");
        }

        [SFFact]
        public void TestCommandPrepareShouldNotThrowsException()
        {
            Assert.DoesNotThrow(() => command.Prepare());
        }
    }
}
