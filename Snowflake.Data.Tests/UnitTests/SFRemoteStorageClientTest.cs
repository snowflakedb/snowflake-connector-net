using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
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
    public sealed class SFRemoteStorageClientTest : IDisposable
    {
        private string TestNameWithWorker => GetType().Name + "_" + Thread.CurrentThread.ManagedThreadId;
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

        public SFRemoteStorageClientTest()
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

        public new void Dispose()
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

        [SFFact(Skip = "RemoteStorageClientTest")]
        public void RemoteStorageClientTestDone()
        {
            // Do nothing;
        }

        [SFTheory]
        [InlineData(SFRemoteStorageUtil.LOCAL_FS)]
        [InlineData(SFRemoteStorageUtil.S3_FS)]
        [InlineData(SFRemoteStorageUtil.AZURE_FS)]
        [InlineData(SFRemoteStorageUtil.GCS_FS)]
        [InlineData(UnsupportedStageType)] // Any other stage type should return null
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
                    Assert.IsType<SFS3Client>(client);
                }
                else if (stageType == SFRemoteStorageUtil.AZURE_FS)
                {
                    Assert.IsType<SFSnowflakeAzureClient>(client);
                }
                else if (stageType == SFRemoteStorageUtil.GCS_FS)
                {
                    Assert.IsType<SFGCSClient>(client);
                }
                else
                {
                    Assert.Null(client);
                }
            }
        }

        [SFTheory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
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
            Assert.Equal(DestFileSizeWhenFileAlreadyExists, _fileMetadata.destFileSize);
            Assert.Equal(ResultStatus.SKIPPED.ToString(), _fileMetadata.resultStatus);
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

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.OK, 1)]
        public void TestUploadOneFileWithRetry(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            SFRemoteStorageUtil.UploadOneFileWithRetry(_fileMetadata);

            // Assert
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.OK, 1)]
        public async Task TestUploadOneFileAsyncWithRetry(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            await SFRemoteStorageUtil.UploadOneFileWithRetryAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, null, 4)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.BadRequest, 6)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.Unauthorized, 5)]
        public void TestUploadOneFile(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatusInt)
        {
            // Arrange
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            SFRemoteStorageUtil.UploadOneFile(_fileMetadata);

            // Assert
            if (expectedResultStatus == ResultStatus.SKIPPED)
            {
                Assert.Equal(DestFileSizeWhenFileAlreadyExists, _fileMetadata.destFileSize);
            }
            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, null, 4)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.BadRequest, 6)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.Unauthorized, 5)]
        public async Task TestUploadOneFileAsync(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatusInt)
        {
            // Arrange
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            await SFRemoteStorageUtil.UploadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            if (expectedResultStatus == ResultStatus.SKIPPED)
            {
                Assert.Equal(DestFileSizeWhenFileAlreadyExists, _fileMetadata.destFileSize);
            }
            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.ServiceUnavailable, 8)]
        public void TestUploadOneFileThrowsForRetryErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            Exception ex = Assert.Throws<WebException>(() => SFRemoteStorageUtil.UploadOneFile(_fileMetadata));

            // Assert
            Assert.Matches(MockRemoteStorageClient.ErrorMessage, ex.Message);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.NotFound, HttpStatusCode.ServiceUnavailable, 8)]
        public async Task TestUploadOneFileAsyncThrowsForRetryErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            Exception ex = await Assert.ThrowsAsync<WebException>(async () => await SFRemoteStorageUtil.UploadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.Matches(MockRemoteStorageClient.ErrorMessage, ex.Message);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }


        [SFTheory]
        [InlineData(HttpStatusCode.NotFound, null, 0)]
        public void TestUploadOneFileThrowsForUnknownErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, NotAsync);

            // Act
            Exception ex = Assert.Throws<Exception>(() => SFRemoteStorageUtil.UploadOneFile(_fileMetadata));

            // Assert
            Assert.Matches($"Unknown Error in uploading a file: .*", ex.Message);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound, null, 0)]
        public async Task TestUploadOneFileAsyncThrowsForUnknownErrors(HttpStatusCode httpStatusCode, HttpStatusCode httpStatusCodeAfterRetry, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForUpload(httpStatusCode, httpStatusCodeAfterRetry, IsAsync);

            // Act
            Exception ex = await Assert.ThrowsAsync<Exception>(async () => await SFRemoteStorageUtil.UploadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.Matches($"Unknown Error in uploading a file: .*", ex.Message);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
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

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        public async Task TestDownloadOneFile(HttpStatusCode httpStatusCode, int expectedResultStatusInt)
        {
            // Arrange
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            SFRemoteStorageUtil.DownloadOneFile(_fileMetadata);

            // Assert
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = await ReadDownloadFileAsync();
                Assert.Equal(MockRemoteStorageClient.FileContent, text);
            }
            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        public async Task TestDownloadOneFileAsync(HttpStatusCode httpStatusCode, int expectedResultStatusInt)
        {
            // Arrange
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                var text = await ReadDownloadFileAsync();
                Assert.Equal(MockRemoteStorageClient.FileContent, text);
            }
            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }


        [SFTheory]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        public void TestDownloadOneFileThrowsForRetryErrors(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            Exception ex = Assert.Throws<WebException>(() => SFRemoteStorageUtil.DownloadOneFile(_fileMetadata));

            // Assert
            Assert.Matches(MockRemoteStorageClient.ErrorMessage, ex.Message);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        public async Task TestDownloadOneFileAsyncThrowsForRetryErrors(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            Exception ex = await Assert.ThrowsAsync<WebException>(async () => await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.Matches(MockRemoteStorageClient.ErrorMessage, ex.Message);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound)]
        public void TestDownloadOneFileThrowsForUnknownErrors(HttpStatusCode httpStatusCode)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            Exception ex = Assert.Throws<Exception>(() => SFRemoteStorageUtil.DownloadOneFile(_fileMetadata));

            // Assert
            Assert.Matches($"Unknown Error in downloading a file: .*", ex.Message);
            Assert.Null(_fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.NotFound)]
        public async Task TestDownloadOneFileAsyncThrowsForUnknownErrors(HttpStatusCode httpStatusCode)
        {
            // Arrange
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            Exception ex = await Assert.ThrowsAsync<Exception>(async () => await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false));

            // Assert
            Assert.Matches($"Unknown Error in downloading a file: .*", ex.Message);
            Assert.Null(_fileMetadata.resultStatus);
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

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        public async Task TestDownloadOneFileWithEncryptionMaterial(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            SetUpMockEncryptedFileForDownload();
            SetUpMockClientForDownload(httpStatusCode, NotAsync);

            // Act
            SFRemoteStorageUtil.DownloadOneFile(_fileMetadata);

            // Assert
            string text = await ReadDownloadFileAsync();
            Assert.Equal(MockRemoteStorageClient.FileContent, text);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        public async Task TestDownloadOneFileAsyncWithEncryptionMaterial(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            SetUpMockEncryptedFileForDownload();
            SetUpMockClientForDownload(httpStatusCode, IsAsync);

            // Act
            await SFRemoteStorageUtil.DownloadOneFileAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            string text = await ReadDownloadFileAsync();
            Assert.Equal(MockRemoteStorageClient.FileContent, text);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        private async Task<string> ReadDownloadFileAsync()
        {
#if NETFRAMEWORK
            var result = File.ReadAllText(t_downloadFileName);
            return result;
#else
            var text = await File.ReadAllTextAsync(t_downloadFileName, _cancellationToken);
            return text;
#endif
        }
    }
}
