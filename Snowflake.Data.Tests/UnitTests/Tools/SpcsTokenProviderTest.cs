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
        private readonly Mock<FileOperations> _fileOperations = new();
        private readonly Mock<IEnvironmentFacade> _environmentFacade = new();

        private SpcsTokenProvider CreateProvider()
        {
            return new SpcsTokenProvider(_fileOperations.Object, _environmentFacade.Object);
        }

        [SFFact]
        public void TestReturnsNullWhenRunningInsideSpcsEnvVarIsNotSet()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns(string.Empty);
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Null(token);
            _fileOperations.Verify(f => f.ReadAllText(It.IsAny<string>()), Times.Never);
        }

        [SFFact]
        public void TestReturnsTokenFromConfiguredPath()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/custom/token/path");
            _fileOperations.Setup(f => f.ReadAllText("/custom/token/path")).Returns("my-spcs-token");
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Equal("my-spcs-token", token);
        }

        [SFFact]
        public void TestTrimsWhitespaceFromToken()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/snowflake/session/spcs_token");
            _fileOperations.Setup(f => f.ReadAllText("/snowflake/session/spcs_token")).Returns("  my-spcs-token\n");
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Equal("my-spcs-token", token);
        }

        [SFFact]
        public void TestReturnsNullWhenFileDoesNotExist()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/snowflake/session/spcs_token");
            _fileOperations.Setup(f => f.ReadAllText("/snowflake/session/spcs_token"))
                .Throws(new FileNotFoundException("File not found"));
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestReturnsNullWhenFileIsEmpty()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/snowflake/session/spcs_token");
            _fileOperations.Setup(f => f.ReadAllText("/snowflake/session/spcs_token")).Returns("");
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestReturnsNullWhenFileContainsOnlyWhitespace()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/snowflake/session/spcs_token");
            _fileOperations.Setup(f => f.ReadAllText("/snowflake/session/spcs_token")).Returns("   \n  ");
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestReturnsNullAndDoesNotThrowOnReadException()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/snowflake/session/spcs_token");
            _fileOperations.Setup(f => f.ReadAllText("/snowflake/session/spcs_token"))
                .Throws(new IOException("Access denied"));
            var provider = CreateProvider();

            // act
            var token = provider.GetSpcsToken();

            // assert
            Assert.Null(token);
        }

        [SFFact]
        public void TestCreateIfRunningInSpcsReturnsNullWhenNotInSpcs()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns(string.Empty);

            // act
            var provider = SpcsTokenProvider.CreateIfRunningInSpcs(_environmentFacade.Object);

            // assert
            Assert.Null(provider);
        }

        [SFFact]
        public void TestCreateIfRunningInSpcsReturnsProviderWhenInSpcs()
        {
            // arrange
            _environmentFacade.Setup(e => e.GetString(EnvVars.RunningInsideSpcs)).Returns("/snowflake/session/spcs_token");

            // act
            var provider = SpcsTokenProvider.CreateIfRunningInSpcs(_environmentFacade.Object);

            // assert
            Assert.NotNull(provider);
        }
    }
}
