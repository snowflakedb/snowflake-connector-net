using System;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
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

    [TestFixture, NonParallelizable]
    class SFAzureClientTest : SFBaseTest
    {
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

        [SetUp]
        public new void BeforeTest()
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

        [Test]
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
            Assert.AreEqual(bucketAndKey[0], location.bucket);
            Assert.AreEqual(bucketAndKey[1], location.key);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.ERROR)]  // Any error that isn't the above will return ResultStatus.ERROR
        public void TestGetFileHeader(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.ERROR)]  // Any error that isn't the above will return ResultStatus.ERROR
        public async Task TestGetFileHeaderAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockAzureClient.ContentLength, fileHeader.contentLength);
                Assert.AreEqual(MockAzureClient.SFCDigest, fileHeader.digest);
                Assert.AreEqual(MockAzureClient.AzureIV, fileHeader.encryptionMetadata.iv);
                Assert.AreEqual(MockAzureClient.AzureKey, fileHeader.encryptionMetadata.key);
                Assert.AreEqual(MockAzureClient.AzureMatdesc, fileHeader.encryptionMetadata.matDesc);
            }
            else
            {
                Assert.IsNull(fileHeader);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public void TestUploadFile(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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
                        mockBlobClient.Setup(client => client.Upload(It.IsAny<Stream>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
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


        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public async Task TestUploadFileAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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
                        mockBlobClient.Setup(client => client.UploadAsync(It.IsAny<Stream>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
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

        private void AssertForUploadFileTests(ResultStatus expectedResultStatus)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(_fileMetadata.uploadSize, _fileMetadata.destFileSize);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public void TestDownloadFile(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }


        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public async Task TestDownloadFileAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
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
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), _fileMetadata.resultStatus);
            Assert.AreEqual("something", fileHeader.digest);
            Assert.AreEqual("initVector", fileHeader.encryptionMetadata.iv);
            Assert.AreEqual("key", fileHeader.encryptionMetadata.key);
            Assert.AreEqual("description", fileHeader.encryptionMetadata.matDesc);
        }

        [Test]
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
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), _fileMetadata.resultStatus);
            Assert.IsNull(fileHeader.digest);
            Assert.AreEqual("initVector", fileHeader.encryptionMetadata.iv);
            Assert.AreEqual("key", fileHeader.encryptionMetadata.key);
            Assert.AreEqual("description", fileHeader.encryptionMetadata.matDesc);
        }

        [Test]
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
            Assert.That(thrown.Message, Does.Contain("The given key 'matdesc' was not present in the dictionary."));
        }
    }
}
