/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    [TestFixture]
    class SFDbCommandTest
    {
        SnowflakeDbCommand command;

        [SetUp]
        public void BeforeTest()
        {
            command = new SnowflakeDbCommand();
        }

        [Test]
        public void TestCommandWithConnectionAndCommandText()
        {
            // Arrange
            SnowflakeDbConnection conn = new SnowflakeDbConnection();
            string commandText = "select 1";

            // Act
            command = new SnowflakeDbCommand(conn, commandText);

            // Assert
            Assert.AreEqual(conn, command.Connection);
            Assert.AreEqual(commandText, command.CommandText);
        }

        [Test]
        public void TestCommandExecuteThrowsExceptionWhenCommandTextIsNotSet()
        {
            // Act
            var thrown = Assert.Throws<Exception>(() => command.ExecuteScalar());

            // Assert
            Assert.AreEqual(thrown.Message, "Unable to execute command due to command text not being set");
        }

        [Test]
        public void TestCommandExecuteAsyncThrowsExceptionWhenCommandTextIsNotSet()
        {
            // Arrange
            Task<object> commandTask = command.ExecuteScalarAsync(CancellationToken.None);

            // Act
            var thrown = Assert.Throws<AggregateException>(() => commandTask.Wait());

            // Assert
            Assert.AreEqual(thrown.InnerException.Message, "Unable to execute command due to command text not being set");
        }

        [Test]
        public void TestCommandPrepareShouldNotThrowsException()
        {
            Assert.DoesNotThrow(() => command.Prepare());
        }
    }
}
