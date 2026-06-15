using System;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer.StorageClient;
    using Snowflake.Data.Core.FileTransfer;
    using System.Collections.Generic;
    using System.Net;
    using Snowflake.Data.Tests.Mock;
    using System.Threading;
    using System.Threading.Tasks;
    using System.IO;
    using Azure.Storage.Blobs;
    using Moq;
    using Azure;
    using Azure.Storage.Blobs.Models;
    public sealed class SFAzureClientTest
    {
        private string TestNameWithWorker => GetType().Name + "_" + Thread.CurrentThread.ManagedThreadId;
        // Mock data for file metadata
        const string EndPoint = "blob.core.windows.net";

        const string LocationStage = "mock-customer-stage";
        const string LocationId = "mock-id";
        const string LocationTables = "tables";
        const string LocationKey = "mock-key";
        const string LocationPath = LocationTables + "/" + LocationKey + "/";
        const string Location = LocationStage + "/" + LocationId + "/" + LocationPath;

        const string Region = "canadacentral";

        Dictionary<string, string> _stageCredentials = new Dictionary<string, string>()
        {
            {"AZURE_SAS_TOKEN", "MOCK_AZURE_SAS_TOKEN"}
        };

        const string StorageAccount = "mockStorageAccount";

        // Settings for mock client
        const int Parallel = 0;

        // File name for download tests
        [ThreadStatic] private static string t_downloadFileName;

        // Token for async tests
        CancellationToken _cancellationToken;

        // Mock upload file size
        const int UploadFileSize = 9999;

        // The mock client and metadata
        SFSnowflakeAzureClient _client;
        SFFileMetadata _fileMetadata;

        public SFAzureClientTest()
        {
            t_downloadFileName = TestNameWithWorker + "_mockFileName.txt";

            _fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = EndPoint,
                    location = Location,
                    locationType = SFRemoteStorageUtil.AZURE_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = Region,
                    stageCredentials = _stageCredentials,
                    storageAccount = StorageAccount
                }
            };

            _cancellationToken = new CancellationToken();
        }

        [SFFact]
        public void TestExtractBucketNameAndPath()
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = Location.Split(new[] { '/' }, 2);

            // Act
            RemoteLocation location = _client.ExtractBucketNameAndPath(_fileMetadata.stageInfo.location);

            // Assert
            Assert.Equal(bucketAndKey[0], location.bucket);
            Assert.Equal(bucketAndKey[1], location.key);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.BadRequest, 5)]
        [InlineData(HttpStatusCode.NotFound, 7)]
        [InlineData(HttpStatusCode.Forbidden, 0)]  // Any error that isn't the above will return 0
        [InlineData(HttpStatusCode.GatewayTimeout, 0)]
        [InlineData(HttpStatusCode.RequestTimeout, 0)]
        [InlineData(HttpStatusCode.BadGateway, 0)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 0)]
        public void TestGetFileHeader(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            mockBlobServiceClient.Setup(client => client.GetBlobContainerClient(It.IsAny<string>()))
                .Returns<string>((key) =>
                {
                    var mockBlobContainerClient = new Mock<BlobContainerClient>();
                    mockBlobContainerClient.Setup(client => client.GetBlobClient(It.IsAny<string>()))
                    .Returns<string>((blobName) =>
                    {
                        var mockBlobClient = new Mock<BlobClient>();
                        mockBlobClient.Setup(client => client.GetProperties(It.IsAny<BlobRequestConditions>(), It.IsAny<CancellationToken>()))
                        .Returns(() => MockAzureClient.createMockResponseForBlobProperties(key));

                        return mockBlobClient.Object;
                    });

                    return mockBlobContainerClient.Object;
                });
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            // Act
            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

            // Assert
            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.BadRequest, 5)]
        [InlineData(HttpStatusCode.NotFound, 7)]
        [InlineData(HttpStatusCode.Forbidden, 0)]  // Any error that isn't the above will return 0
        public async Task TestGetFileHeaderAsync(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            mockBlobServiceClient.Setup(client => client.GetBlobContainerClient(It.IsAny<string>()))
                .Returns<string>((key) =>
                {
                    var mockBlobContainerClient = new Mock<BlobContainerClient>();
                    mockBlobContainerClient.Setup(client => client.GetBlobClient(It.IsAny<string>()))
                    .Returns<string>((blobName) =>
                    {
                        var mockBlobClient = new Mock<BlobClient>();
                        mockBlobClient.Setup(client => client.GetPropertiesAsync(It.IsAny<BlobRequestConditions>(), It.IsAny<CancellationToken>()))
                        .Returns(async () => await Task.Run(() => MockAzureClient.createMockResponseForBlobProperties(key)).ConfigureAwait(false));

                        return mockBlobClient.Object;
                    });

                    return mockBlobContainerClient.Object;
                });
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            // Act
            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(int expectedResultStatusInt, FileHeader fileHeader)
        {
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.Equal(MockAzureClient.ContentLength, fileHeader.contentLength);
                Assert.Equal(MockAzureClient.SFCDigest, fileHeader.digest);
                Assert.Equal(MockAzureClient.AzureIV, fileHeader.encryptionMetadata.iv);
                Assert.Equal(MockAzureClient.AzureKey, fileHeader.encryptionMetadata.key);
                Assert.Equal(MockAzureClient.AzureMatdesc, fileHeader.encryptionMetadata.matDesc);
            }
            else
            {
                Assert.Null(fileHeader);
                Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
            }
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.BadRequest, 6)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(HttpStatusCode.BadGateway, 0)]
        [InlineData(HttpStatusCode.GatewayTimeout, 0)]
        public void TestUploadFile(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            mockBlobServiceClient.Setup(client => client.GetBlobContainerClient(It.IsAny<string>()))
                .Returns<string>((key) =>
                {
                    var mockBlobContainerClient = new Mock<BlobContainerClient>();
                    mockBlobContainerClient.Setup(client => client.GetBlobClient(It.IsAny<string>()))
                    .Returns<string>((blobName) =>
                    {
                        var mockBlobClient = new Mock<BlobClient>();
                        mockBlobClient.Setup(client => client.Upload(It.IsAny<Stream>(), It.IsAny<BlobUploadOptions>(), It.IsAny<CancellationToken>()))
                        .Returns(() => MockAzureClient.createMockResponseForBlobContentInfo(key));

                        return mockBlobClient.Object;
                    });

                    return mockBlobContainerClient.Object;
                });
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();
            _fileMetadata.uploadSize = UploadFileSize;

            // Act
            _client.UploadFile(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockAzureClient.AzureIV,
                key = MockAzureClient.AzureKey,
                matDesc = MockAzureClient.AzureMatdesc
            });

            // Assert
            AssertForUploadFileTests(expectedResultStatus);
        }


        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.BadRequest, 6)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(HttpStatusCode.BadGateway, 0)]
        [InlineData(HttpStatusCode.GatewayTimeout, 0)]
        [InlineData(HttpStatusCode.TemporaryRedirect, 0)]
        public async Task TestUploadFileAsync(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            mockBlobServiceClient.Setup(client => client.GetBlobContainerClient(It.IsAny<string>()))
                .Returns<string>((key) =>
                {
                    var mockBlobContainerClient = new Mock<BlobContainerClient>();
                    mockBlobContainerClient.Setup(client => client.GetBlobClient(It.IsAny<string>()))
                    .Returns<string>((blobName) =>
                    {
                        var mockBlobClient = new Mock<BlobClient>();
                        mockBlobClient.Setup(client => client.UploadAsync(It.IsAny<Stream>(), It.IsAny<BlobUploadOptions>(), It.IsAny<CancellationToken>()))
                        .Returns(async () => await Task.Run(() => MockAzureClient.createMockResponseForBlobContentInfo(key)).ConfigureAwait(false));

                        return mockBlobClient.Object;
                    });

                    return mockBlobContainerClient.Object;
                });
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();
            _fileMetadata.uploadSize = UploadFileSize;

            // Act
            await _client.UploadFileAsync(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockAzureClient.AzureIV,
                key = MockAzureClient.AzureKey,
                matDesc = MockAzureClient.AzureMatdesc
            },
            _cancellationToken).ConfigureAwait(false);

            // Assert
            AssertForUploadFileTests(expectedResultStatus);
        }

        private void AssertForUploadFileTests(int expectedResultStatus)
        {
            if ((ResultStatus)expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.Equal(_fileMetadata.uploadSize, _fileMetadata.destFileSize);
            }

            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(HttpStatusCode.BadGateway, 0)]
        [InlineData(HttpStatusCode.GatewayTimeout, 0)]
        [InlineData(HttpStatusCode.NotFound, 0)]
        public void TestDownloadFile(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            mockBlobServiceClient.Setup(client => client.GetBlobContainerClient(It.IsAny<string>()))
                .Returns<string>((key) =>
                {
                    var mockBlobContainerClient = new Mock<BlobContainerClient>();
                    mockBlobContainerClient.Setup(client => client.GetBlobClient(It.IsAny<string>()))
                    .Returns<string>((blobName) =>
                    {
                        var mockBlobClient = new Mock<BlobClient>();
                        mockBlobClient.Setup(client => client.DownloadTo(It.IsAny<Stream>()))
                        .Returns(() =>
                        {
                            if (key == HttpStatusCode.OK.ToString())
                            {
                                return null;
                            }
                            else
                            {
                                throw MockAzureClient.CreateMockAzureError(key);
                            }
                        });

                        return mockBlobClient.Object;
                    });

                    return mockBlobContainerClient.Object;
                });
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            // Act
            _client.DownloadFile(_fileMetadata, t_downloadFileName, Parallel);

            // Assert
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }


        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(HttpStatusCode.BadGateway, 0)]
        [InlineData(HttpStatusCode.GatewayTimeout, 0)]
        [InlineData(HttpStatusCode.RequestTimeout, 0)]
        public async Task TestDownloadFileAsync(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            mockBlobServiceClient.Setup(client => client.GetBlobContainerClient(It.IsAny<string>()))
                .Returns<string>((key) =>
                {
                    var mockBlobContainerClient = new Mock<BlobContainerClient>();
                    mockBlobContainerClient.Setup(client => client.GetBlobClient(It.IsAny<string>()))
                    .Returns<string>((blobName) =>
                    {
                        var mockBlobClient = new Mock<BlobClient>();
                        mockBlobClient.Setup(client => client.DownloadToAsync(It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
                        .Returns(async () =>
                        {
                            if (key == HttpStatusCode.OK.ToString())
                            {
                                return await Task.Run(() => Task.FromResult<Response>(null)).ConfigureAwait(false);
                            }
                            else
                            {
                                throw MockAzureClient.CreateMockAzureError(key);
                            }
                        });

                        return mockBlobClient.Object;
                    });

                    return mockBlobContainerClient.Object;
                });
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            // Act
            await _client.DownloadFileAsync(_fileMetadata, t_downloadFileName, Parallel, _cancellationToken).ConfigureAwait(false);

            // Assert
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFFact]
        public void TestEncryptionMetadataReadingIsCaseInsensitive()
        {
            // arrange
            var metadata = new Dictionary<string, string>
            {
                {
                    "ENCRYPTIONDATA",
                    @"{
                        ""ContentEncryptionIV"": ""initVector"",
                        ""WrappedContentKey"": {
                            ""EncryptedKey"": ""key""
                        }
                    }"
                },
                { "MATDESC", "description" },
                { "SFCDIGEST", "something"}
            };
            var blobProperties = BlobsModelFactory.BlobProperties(metadata: metadata, contentLength: 10);
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);

            // act
            var fileHeader = _client.HandleFileHeaderResponse(ref _fileMetadata, blobProperties);

            // assert
            Assert.Equal("something", fileHeader.digest);
            Assert.Equal("initVector", fileHeader.encryptionMetadata.iv);
            Assert.Equal("key", fileHeader.encryptionMetadata.key);
            Assert.Equal("description", fileHeader.encryptionMetadata.matDesc);
        }

        [SFFact]
        public void TestEncryptionMetadataReadingSucceedsWithoutSfcDigest()
        {
            // arrange
            var metadata = new Dictionary<string, string>
            {
                {
                    "encryptiondata",
                    @"{
                        ""ContentEncryptionIV"": ""initVector"",
                        ""WrappedContentKey"": {
                            ""EncryptedKey"": ""key""
                        }
                    }"
                },
                { "matdesc", "description" }
            };
            var blobProperties = BlobsModelFactory.BlobProperties(metadata: metadata, contentLength: 10);
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);

            // act
            var fileHeader = _client.HandleFileHeaderResponse(ref _fileMetadata, blobProperties);

            // assert
            Assert.Null(fileHeader.digest);
            Assert.Equal("initVector", fileHeader.encryptionMetadata.iv);
            Assert.Equal("key", fileHeader.encryptionMetadata.key);
            Assert.Equal("description", fileHeader.encryptionMetadata.matDesc);
        }

        [SFFact]
        public void TestHandleFileHeaderResponseDoesNotOverwriteResultStatus()
        {
            // arrange - simulate the download path: status is already set to DOWNLOADED before GetFileHeader is called
            var metadata = new Dictionary<string, string>
            {
                {
                    "encryptiondata",
                    @"{
                        ""ContentEncryptionIV"": ""initVector"",
                        ""WrappedContentKey"": {
                            ""EncryptedKey"": ""key""
                        }
                    }"
                },
                { "matdesc", "description" }
            };
            var blobProperties = BlobsModelFactory.BlobProperties(metadata: metadata, contentLength: 10);
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);
            _fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();

            // act
            _client.HandleFileHeaderResponse(ref _fileMetadata, blobProperties);

            // assert - DOWNLOADED must not be overwritten with UPLOADED
            Assert.Equal(ResultStatus.DOWNLOADED.ToString(), _fileMetadata.resultStatus);
        }

        [SFFact]
        public void TestEncryptionMetadataReadingFailsWhenMandatoryPropertyIsMissing()
        {
            // arrange
            var metadataWithoutMatDesc = new Dictionary<string, string>
            {
                {
                    "encryptiondata",
                    @"{
                        ""ContentEncryptionIV"": ""initVector"",
                        ""WrappedContentKey"": {
                            ""EncryptedKey"": ""key""
                        }
                    }"
                }
            };
            var blobProperties = BlobsModelFactory.BlobProperties(metadata: metadataWithoutMatDesc, contentLength: 10);
            var mockBlobServiceClient = new Mock<BlobServiceClient>();
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, mockBlobServiceClient.Object);

            // act
            var thrown = Assert.Throws<KeyNotFoundException>(() => _client.HandleFileHeaderResponse(ref _fileMetadata, blobProperties));

            // assert
            Assert.Contains("The given key 'matdesc' was not present in the dictionary.", thrown.Message);
        }
    }
}
