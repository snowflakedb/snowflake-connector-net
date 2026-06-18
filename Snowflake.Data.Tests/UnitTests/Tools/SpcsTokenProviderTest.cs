using System;
using System.IO;
using Moq;
using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class SpcsTokenProviderTest
    {
        public SpcsTokenProviderTest()
        {
            Setup();
        }

        [ThreadStatic]
        private static Mock<FileOperations> t_fileOperations;

        [ThreadStatic]
        private static Mock<EnvironmentOperations> t_environmentOperations;

        [ThreadStatic]
        private static SpcsTokenProvider t_provider;

        private void Setup()
        {
            t_fileOperations = new Mock<FileOperations>();
            t_environmentOperations = new Mock<EnvironmentOperations>();
            t_provider = new SpcsTokenProvider(t_fileOperations.Object, t_environmentOperations.Object);
        }

        [SFFact]
        public void TestReturnsNullWhenRunningInsideSpcsEnvVarIsNotSet()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns((string)null);

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Null(token);
            t_fileOperations.Verify(f => f.ReadAllText(It.IsAny<string>()), Times.Never);
        }

        [SFFact]
        public void TestReturnsNullWhenRunningInsideSpcsEnvVarIsEmpty()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("");

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Null(token);
            t_fileOperations.Verify(f => f.ReadAllText(It.IsAny<string>()), Times.Never);
        }

        [SFFact]
        public void TestReturnsTokenFromDefaultPath()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("true");
            t_fileOperations.Setup(f => f.ReadAllText(SpcsTokenProvider.DefaultSpcsTokenPath))
                .Returns("my-spcs-token");

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Equal("my-spcs-token", token);
        }

        [SFFact]
        public void TestTrimsWhitespaceFromToken()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("true");
            t_fileOperations.Setup(f => f.ReadAllText(SpcsTokenProvider.DefaultSpcsTokenPath))
                .Returns("  my-spcs-token\n");

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Equal("my-spcs-token", token);
        }

        [SFFact]
        public void TestReturnsNullWhenFileDoesNotExist()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("true");
            t_fileOperations.Setup(f => f.ReadAllText(SpcsTokenProvider.DefaultSpcsTokenPath))
                .Throws(new FileNotFoundException("File not found", SpcsTokenProvider.DefaultSpcsTokenPath));

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestReturnsNullWhenFileIsEmpty()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("true");
            t_fileOperations.Setup(f => f.ReadAllText(SpcsTokenProvider.DefaultSpcsTokenPath))
                .Returns("");

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestReturnsNullWhenFileContainsOnlyWhitespace()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("true");
            t_fileOperations.Setup(f => f.ReadAllText(SpcsTokenProvider.DefaultSpcsTokenPath))
                .Returns("   \n  ");

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestReturnsNullAndDoesNotThrowOnReadException()
        {
            // arrange
            t_environmentOperations.Setup(e => e.GetEnvironmentVariable(SpcsTokenProvider.RunningInsideSpcsEnvVar))
                .Returns("true");
            t_fileOperations.Setup(f => f.ReadAllText(SpcsTokenProvider.DefaultSpcsTokenPath))
                .Throws(new IOException("Access denied"));

            // act
            var token = t_provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }
    }
}
