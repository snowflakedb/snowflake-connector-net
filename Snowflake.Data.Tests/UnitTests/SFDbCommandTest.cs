using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests;

using Xunit;
using Snowflake.Data.Client;
using System;
using System.Threading;

public sealed class SFDbCommandTest
{
    private SnowflakeDbCommand _command;

    public SFDbCommandTest()
    {
        _command = new SnowflakeDbCommand();
    }

    [SFFact]
    public void TestCommandWithConnectionAndCommandText()
    {
        // Arrange
        var conn = new SnowflakeDbConnection();
        var commandText = "select 1";

        // Act
        _command = new SnowflakeDbCommand(conn, commandText);

        // Assert
        Assert.Equal(conn, _command.Connection);
        Assert.Equal(commandText, _command.CommandText);
    }

    [SFFact]
    public void TestCommandExecuteThrowsExceptionWhenCommandTextIsNotSet()
    {
        // Act
        var thrown = Assert.Throws<Exception>(() => _command.ExecuteScalar());

        // Assert
        Assert.NotEmpty(thrown.Message);
    }

    [SFFact]
    public void TestCommandExecuteAsyncThrowsExceptionWhenCommandTextIsNotSet()
    {
        // Arrange
        var commandTask = _command.ExecuteScalarAsync(CancellationToken.None);

        // Act
        var thrown = Assert.Throws<AggregateException>(() => commandTask.Wait());

        // Assert
        Assert.Equal("Unable to execute command due to command text not being set", thrown.InnerException.Message);
    }

    [SFFact]
    public void TestCommandPrepareShouldNotThrowsException()
    {
        _command.Prepare();
    }
}
