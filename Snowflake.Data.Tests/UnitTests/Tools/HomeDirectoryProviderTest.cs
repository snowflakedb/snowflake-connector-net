/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.Tools
{
    [TestFixture]
    public class HomeDirectoryProviderTest
    {
        private const string HomeDirectory = "/home/user";

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        [SetUp]
        public void Setup()
        {
            t_environmentOperations = new Mock<EnvironmentOperations>();
        }

        [Test]
        public void TestThatReturnsHomeDirectorySuccessfully()
        {
            // arrange
            MockHomeDirectory();

            // act
            var actualHomeDirectory = HomeDirectoryProvider.HomeDirectory(t_environmentOperations.Object);

            // assert
            Assert.AreEqual(HomeDirectory, actualHomeDirectory);
        }

        [Test]
        public void TestThatDoesNotFailWhenHomeDirectoryReturnsNull()
        {
            // arrange
            MockHomeDirectoryReturnsNull();

            // act
            var actualHomeDirectory = HomeDirectoryProvider.HomeDirectory(t_environmentOperations.Object);

            // assert
            Assert.IsNull(actualHomeDirectory);
            t_environmentOperations.Verify(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile), Times.Once);
        }

        [Test]
        public void TestThatDoesNotFailWhenHomeDirectoryThrowsError()
        {
            // arrange
            MockHomeDirectoryFails();

            // act
            var actualHomeDirectory = HomeDirectoryProvider.HomeDirectory(t_environmentOperations.Object);

            // assert
            Assert.IsNull(actualHomeDirectory);
            t_environmentOperations.Verify(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile), Times.Once);
        }

        private static void MockHomeDirectory()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns(HomeDirectory);
        }

        private static void MockHomeDirectoryReturnsNull()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns((string)null);
        }

        private static void MockHomeDirectoryFails()
        {
            t_environmentOperations
                .Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Throws(() => new Exception("No home directory"));
        }
    }
}
