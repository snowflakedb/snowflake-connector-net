﻿/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System;

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
        public void TestCommandPrepareThrowsNotImplemented()
        {
            Assert.Throws<NotImplementedException>(() => command.Prepare());
        }
    }
}
