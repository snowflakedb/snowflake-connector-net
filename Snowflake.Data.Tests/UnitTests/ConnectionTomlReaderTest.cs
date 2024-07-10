/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;

    [TestFixture, NonParallelizable]
    class ConnectionTomlReaderTest
    {

        [Test]
        [Ignore("Pending to mock filesystem for testing")]
        public void Test()
        {
            // Arrange
            var reader = new SnowflakeTomlConnectionBuilder();

            // Act
            var connectionString = reader.GetConnectionStringFromToml("testconnection");

            // Assert
            Assert.AreEqual("", connectionString);
        }

    }

}
