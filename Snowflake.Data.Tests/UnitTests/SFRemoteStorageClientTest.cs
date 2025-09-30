namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer.StorageClient;
    using Snowflake.Data.Core.FileTransfer;
    using System.Collections.Generic;
    using System;
    using Snowflake.Data.Tests.Mock;
    using System.Threading.Tasks;
    using System.Threading;
    using System.IO;
    using System.Text;
    using System.Net;
    using Moq;

    [TestFixture]
    class SFRemoteStorageClientTest : SFBaseTest
    {
        // Mock data for file metadata
        const string EndPoint = "mockEndPoint.com";

        const string LocationStage = "mock-customer-stage";
        const string LocationId = "mock-id";
        const string LocationTables = "tables";
        const string LocationKey = "mock-key";
        const string LocationPath = LocationTables + "/" + LocationKey + "/";
        const string Location = LocationStage + "/" + LocationId + "/" + LocationPath;

        const string Region = "us-west-2";

        const string StorageAccount = "mockStorageAccount";

        [ThreadStatic] private static string t_realSourceFilePath;

        PutGetEncryptionMaterial EncryptionMaterial = new PutGetEncryptionMaterial()
        {
            queryId = "MOCK/QUERY/ID/==",
            queryStageMasterKey = "MOCKQUERYSTAGEMASTERKE==",
            smkId = 9999
        };

        // Mock unsupported stage type
        const string UnsupportedStageType = "UNSUPPORTED";

        // Settings for mock client
        const int Parallel = 0;

        // File name for mock test files
        [ThreadStatic] private static string t_downloadFileName;
        const string LocalLocation = "./";

        // Mock upload file size
        const int UploadFileSize = 9999;
        const int DestFileSizeWhenFileAlreadyExists = 0;

        // Mock client and metadata
        SFGCSClient _client;
        PutGetResponseData _responseData;
        SFFileMetadata _fileMetadata;

        // Token for async tests
        CancellationToken _cancellationToken;

        // Flags for non-async and async mock methods
        const bool NotAsync = false;
        const bool IsAsync = true;

        [SetUp]
        public new void BeforeTest()
        {
            t_realSourceFilePath = TestNameWithWorker + "_realSrcFilePath.txt";
            t_downloadFileName = TestNameWithWorker + "_mockFileName.txt";

            _fileMetadata = new SFFileMetadata()
            {
                destFileName = t_downloadFileName,
                localLocation = LocalLocation,
                MaxBytesInMemory = 1024,
                memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(MockRemoteStorageClient.FileContent)),
                parallel = Parallel,
                realSrcFilePath = t_realSourceFilePath,
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = EndPoint,
                    location = Location,
                    locationType = SFRemoteStorageUtil.GCS_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = Region,
                    stageCredentials = new Dictionary<string, string>()
                    {
                        {"AWS_KEY_ID", "MOCK_AWS_KEY_ID"},
                        {"AWS_SECRET_KEY", "MOCK_AWS_SECRET_KEY"},
                        {"AWS_TOKEN", "MOCK_AWS_TOKEN"},
                        {"AWS_ID", "MOCK_AWS_ID"},
                        {"AWS_KEY", "MOCK_AWS_KEY"},
                        {"AZURE_SAS_TOKEN", "MOCK_AZURE_SAS_TOKEN"},
                        {"GCS_ACCESS_TOKEN", "MOCK_GCS_ACCESS_TOKEN"}
                    },
                    storageAccount = StorageAccount
                },
                uploadSize = UploadFileSize
            };

            _responseData = new PutGetResponseData()
            {
                autoCompress = true,
                stageInfo = _fileMetadata.stageInfo
            };

            // Set the mock GCS client to use
            _client = new SFGCSClient(_fileMetadata.stageInfo);
            _fileMetadata.client = _client;

            _cancellationToken = new CancellationToken();
        }

        [TearDown]
        public new void AfterTest()
        {
            // Delete temporary files from upload
            if (File.Exists(_fileMetadata.realSrcFilePath))
            {
                File.Delete(_fileMetadata.realSrcFilePath);
            }

            // Delete temporary files from download
            if (File.Exists(t_downloadFileName))
            {
                File.Delete(t_downloadFileName);
            }
        }

        [Test]
        [Ignore("RemoteStorageClientTest")]
        public void RemoteStorageClientTestDone()
        {
            // Do nothing;
        }

        [Test]
        [TestCase(SFRemoteStorageUtil.LOCAL_FS)]
        [TestCase(SFRemoteStorageUtil.S3_FS)]
        [TestCase(SFRemoteStorageUtil.AZURE_FS)]
        [TestCase(SFRemoteStorageUtil.GCS_FS)]
        [TestCase(UnsupportedStageType)] // Any other stage type should return null
        public void TestGetRemoteStorageClient(string stageType)
        {
            _responseData.stageInfo.locationType = stageType;

            if (stageType == SFRemoteStorageUtil.LOCAL_FS)
            {
                Assert.Throws<NotImplementedException>(() => SFRemoteStorageUtil.GetRemoteStorage(_responseData));
            }
            else
            {
                ISFRemoteStorageClient client = SFRemoteStorageUtil.GetRemoteStorage(_responseData);

                if (stageType == SFRemoteStorageUtil.S3_FS)
                {
                    Assert.IsInstanceOf<SFS3Client>(client);
                }
                else if (stageType == SFRemoteStorageUtil.AZURE_FS)
                {
                    Assert.IsInstanceOf<SFSnowflakeAzureClient>(client);
                }
                else if (stageType == SFRemoteStorageUtil.GCS_FS)
                {
                    Assert.IsInstanceOf<SFGCSClient>(client);
                }
                else
                {
                    Assert.IsNull(client);
                }
            }
        }

        [Test]
        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void TestUploadFileOrStreamWithAndWithoutEncryption(bool containsEncryptionMaterial, bool useMemoryStream)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(client => client.GetResponse())
                .Returns(() =>
                {
                    return MockRemoteStorageClient.CreateResponseForFileHeader(HttpStatusCode.OK);
                });
            _client.SetCustomWebRequest(mockWebRequest.Object);

            // Add encryption material to the file metadata
            _fileMetadata.encryptionMaterial = containsEncryptionMaterial ? EncryptionMaterial : null;

            // Use file for upload
            if (!useMemoryStream)
            {
                File.WriteAllText(_fileMetadata.realSrcFilePath, MockRemoteStorageClient.FileContent);
                _fileMetadata.memoryStream = null;
            }

            // Act
            SFRemoteStorageUtil.UploadOneFile(_fileMetadata);

            // Assert
            Assert.AreEqual(DestFileSizeWhenFileAlreadyExists, _fileMetadata.destFileSize);
            Assert.AreEqual(ResultStatus.SKIPPED.ToString(), _fileMetadata.resultStatus);
        }

        private Mock<WebRequest> CreateBaseMockClient()
        {
            // Setup the mock GCS client for remote storage tests
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(c => c.Headers).Returns(new WebHeaderCollection());

            return mockWebRequest;
        }

        private void SetUpMockClientForUpload(HttpStatusCode statusCode, HttpStatusCode statusCodeAfterRetry, bool isAsync)
        {
            // Setup the mock GCS client for remote storage tests
            var mockWebRequest = CreateBaseMockClient();
            bool firstRequest = true;

            if (isAsync)
            {
                mockWebRequest.Setup(client => client.GetRequestStreamAsync())
                    .Returns(() => Task.FromResult<Stream>(new MemoryStream()));
                mockWebRequest.Setup(client => client.GetResponseAsync())
                    .Returns(() =>
                    {
                        return Task.FromResult<WebResponse>(MockRemoteStorageClient.SequenceResponseForUploadFile(ref firstRequest, statusCode, statusCodeAfterRetry));
                    });
            }
            else
            {
                mockWebRequest.Setup(client => client.GetRequestStream())
                    .Returns(() => new MemoryStream());
                mockWebRequest.Setup(client => client.GetResponse())
                    .Returns(() =>
                    {
                        return MockRemoteStorageClient.SequenceResponseForUploadFile(ref firstRequest, statusCode, statusCodeAfterRetry);
                    });
            }

            _client.SetCustomWebRequest(mockWebRequest.Object);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.OK, ResultStatus.UPLOADED)]
        public void TestUploadOneFileWithRetry(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            SFRemoteStorageUtil.UploadOneFileWithRetry(_fileMetadata);

            // Assert
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.OK, ResultStatus.UPLOADED)]
        public async Task TestUploadOneFileAsyncWithRetry(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            await SFRemoteStorageUtil.UploadOneFileWithRetryAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, null, ResultStatus.SKIPPED)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        public void TestUploadOneFile(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            SFRemoteStorageUtil.UploadOneFile(_fileMetadata);

            // Assert
            if (expectedResultStatus == ResultStatus.SKIPPED)
            {
                Assert.AreEqual(DestFileSizeWhenFileAlreadyExists, _fileMetadata.destFileSize);
            }
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, null, ResultStatus.SKIPPED)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        public async Task TestUploadOneFileAsync(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            await SFRemoteStorageUtil.UploadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            if (expectedResultStatus == ResultStatus.SKIPPED)
            {
                Assert.AreEqual(DestFileSizeWhenFileAlreadyExists, _fileMetadata.destFileSize);
            }
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public void TestUploadOneFileThrowsForRetryErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            Exception ex = Assert.Throws<WebException>(() => SFRemoteStorageUtil.UploadOneFile(_fileMetadata));

            // Assert
            Assert.That(ex.Message, Does.Match(MockRemoteStorageClient.ErrorMessage));
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.NotFound, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public void TestUploadOneFileAsyncThrowsForRetryErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            Exception ex = Assert.ThrowsAsync<WebException>(async () => await SFRemoteStorageUtil.UploadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.That(ex.Message, Does.Match(MockRemoteStorageClient.ErrorMessage));
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }


        [Test]
        [TestCase(HttpStatusCode.NotFound, null, ResultStatus.ERROR)]
        public void TestUploadOneFileThrowsForUnknownErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            Exception ex = Assert.Throws<Exception>(() => SFRemoteStorageUtil.UploadOneFile(_fileMetadata));

            // Assert
            Assert.That(ex.Message, Does.Match($"Unknown Error in uploading a file: .*"));
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound, null, ResultStatus.ERROR)]
        public void TestUploadOneFileAsyncThrowsForUnknownErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            Exception ex = Assert.ThrowsAsync<Exception>(async () => await SFRemoteStorageUtil.UploadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.That(ex.Message, Does.Match($"Unknown Error in uploading a file: .*"));
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        private void SetUpMockClientForDownload(HttpStatusCode statusCode, bool isAsync)
        {
            // Setup the mock GCS client for remote storage tests
            var mockWebRequest = CreateBaseMockClient();
            if (isAsync)
            {
                mockWebRequest.Setup(client => client.GetResponseAsync())
                    .Returns(() =>
                    {
                        return Task.FromResult<WebResponse>(MockRemoteStorageClient.CreateResponseForDownloadFile(statusCode));
                    });

            }
            else
            {
                mockWebRequest.Setup(client => client.GetResponse())
                .Returns(() =>
                {
                    return MockRemoteStorageClient.CreateResponseForDownloadFile(statusCode);
                });
            }

            _client.SetCustomWebRequest(mockWebRequest.Object);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        public void TestDownloadOneFile(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            SFRemoteStorageUtil.DownloadOneFile(_fileMetadata);

            // Assert
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(t_downloadFileName);
                Assert.AreEqual(MockRemoteStorageClient.FileContent, text);
            }
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        public async Task TestDownloadOneFileAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(t_downloadFileName);
                Assert.AreEqual(MockRemoteStorageClient.FileContent, text);
            }
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public void TestDownloadOneFileThrowsForRetryErrors(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            Exception ex = Assert.Throws<WebException>(() => SFRemoteStorageUtil.DownloadOneFile(_fileMetadata));

            // Assert
            Assert.That(ex.Message, Does.Match(MockRemoteStorageClient.ErrorMessage));
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public void TestDownloadOneFileAsyncThrowsForRetryErrors(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            Exception ex = Assert.ThrowsAsync<WebException>(async () => await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.That(ex.Message, Does.Match(MockRemoteStorageClient.ErrorMessage));
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound)]
        public void TestDownloadOneFileThrowsForUnknownErrors(HttpStatusCode httpStatusCode)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            Exception ex = Assert.Throws<Exception>(() => SFRemoteStorageUtil.DownloadOneFile(_fileMetadata));

            // Assert
            Assert.That(ex.Message, Does.Match($"Unknown Error in downloading a file: .*"));
            Assert.IsNull(_fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.NotFound)]
        public void TestDownloadOneFileAsyncThrowsForUnknownErrors(HttpStatusCode httpStatusCode)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            Exception ex = Assert.ThrowsAsync<Exception>(async () => await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.That(ex.Message, Does.Match($"Unknown Error in downloading a file: .*"));
            Assert.IsNull(_fileMetadata.resultStatus);
        }

        private void SetUpMockEncryptedFileForDownload()
        {
            _fileMetadata.encryptionMaterial = EncryptionMaterial;

            // Write file to encrypt
            File.WriteAllText(t_downloadFileName, MockRemoteStorageClient.FileContent);

            // Get encrypted stream from file
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata();
            StreamPair streamPair = EncryptionProvider.EncryptFile(
                t_downloadFileName,
                _fileMetadata.encryptionMaterial,
                encryptionMetadata,
                FileTransferConfiguration.FromFileMetadata(_fileMetadata));

            // Set up the stream and metadata for decryption
            MockRemoteStorageClient.SetEncryptionData(streamPair.MainStream, encryptionMetadata.iv, encryptionMetadata.key);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        public void TestDownloadOneFileWithEncryptionMaterial(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockEncryptedFileForDownload();
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            SFRemoteStorageUtil.DownloadOneFile(_fileMetadata);

            // Assert
            string text = File.ReadAllText(t_downloadFileName);
            Assert.AreEqual(MockRemoteStorageClient.FileContent, text);
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        public async Task TestDownloadOneFileAsyncWithEncryptionMaterial(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            SetUpMockEncryptedFileForDownload();
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            string text = File.ReadAllText(t_downloadFileName);
            Assert.AreEqual(MockRemoteStorageClient.FileContent, text);
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
