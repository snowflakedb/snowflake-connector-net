using System;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using Snowflake.Data.Core.FileTransfer;
using System.Collections.Generic;
using System.Net;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading;
using Snowflake.Data.Tests.Mock;
using Moq;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class SFGCSClientTest
    {
        private string TestNameWithWorker => GetType().Name + "_" + Thread.CurrentThread.ManagedThreadId;
        // Mock data for file metadata
        const string LocationStage = "mock-customer-stage";
        const string LocationId = "mock-id";
        const string LocationTables = "tables";
        const string LocationKey = "mock-key";
        const string LocationPath = LocationTables + "/" + LocationKey + "/";
        const string Location = LocationStage + "/" + LocationId + "/" + LocationPath;

        const string Region = "US-CENTRAL1";

        Dictionary<string, string> _stageCredentials = new Dictionary<string, string>()
        {
            {"GCS_ACCESS_TOKEN", "MOCK_GCS_ACCESS_TOKEN"}
        };

        // Settings for mock client
        const int Parallel = 0;

        // File name for download tests
        [ThreadStatic] private static string t_downloadFileName;

        // Token for async tests
        CancellationToken _cancellationToken;

        // Mock upload file size
        const int UploadFileSize = 9999;

        // The mock client and metadata
        SFGCSClient _client;
        SFFileMetadata _fileMetadata;

        public SFGCSClientTest()
        {
            t_downloadFileName = TestNameWithWorker + "_mockFileName.txt";

            _fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = null,
                    location = Location,
                    locationType = SFRemoteStorageUtil.GCS_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = Region,
                    stageCredentials = _stageCredentials,
                    storageAccount = null
                }
            };

            // Setup mock GCS HTTP client
            _client = new SFGCSClient(_fileMetadata.stageInfo);

            _cancellationToken = new CancellationToken();
        }

        [SFFact]
        public void TestExtractBucketNameAndPath()
        {
            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = Location.Split(new[] { '/' }, 2);

            RemoteLocation location = _client.ExtractBucketNameAndPath(_fileMetadata.stageInfo.location);

            Assert.Equal(bucketAndKey[0], location.bucket);
            Assert.Equal(bucketAndKey[1], location.key);
        }

        [SFTheory]
        [InlineData(1)]
        [InlineData(2)]
        public void TestGetFileHeaderWhenFileHeaderAlreadyExists(int expectedResultStatus)
        {
            // Setup file metadata
            _fileMetadata.resultStatus = ((ResultStatus)expectedResultStatus).ToString();
            _fileMetadata.sha256Digest = MockGCSClient.SFCDigest;
            _fileMetadata.srcFileSize = MockGCSClient.ContentLength;
            _fileMetadata.encryptionMetadata = new SFEncryptionMetadata()
            {
                iv = MockGCSClient.GcsIV,
                key = MockGCSClient.GcsKey,
                matDesc = MockGCSClient.GcsMatdesc
            };

            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

            AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(expectedResultStatus, fileHeader);
        }

        [SFTheory]
        [InlineData(1)]
        [InlineData(2)]
        public async Task TestGetFileHeaderAsyncWhenFileHeaderAlreadyExists(int expectedResultStatus)
        {
            // Setup file metadata
            _fileMetadata.resultStatus = ((ResultStatus)expectedResultStatus).ToString();
            _fileMetadata.sha256Digest = MockGCSClient.SFCDigest;
            _fileMetadata.srcFileSize = MockGCSClient.ContentLength;
            _fileMetadata.encryptionMetadata = new SFEncryptionMetadata()
            {
                iv = MockGCSClient.GcsIV,
                key = MockGCSClient.GcsKey,
                matDesc = MockGCSClient.GcsMatdesc
            };

            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(int expectedResultStatus, FileHeader fileHeader)
        {
            Assert.Equal(MockGCSClient.ContentLength, fileHeader.contentLength);
            Assert.Equal(MockGCSClient.SFCDigest, fileHeader.digest);
            Assert.Equal(MockGCSClient.GcsIV, fileHeader.encryptionMetadata.iv);
            Assert.Equal(MockGCSClient.GcsKey, fileHeader.encryptionMetadata.key);
            Assert.Equal(MockGCSClient.GcsMatdesc, fileHeader.encryptionMetadata.matDesc);
            Assert.Equal(((ResultStatus)expectedResultStatus).ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(HttpStatusCode.NotFound, 7)]
        [InlineData(HttpStatusCode.Conflict, 0)] // Any error that isn't the above will return ResultStatus.ERROR
        public void TestGetFileHeader(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(client => client.GetResponse())
                .Returns(() =>
                {
                    return MockGCSClient.CreateResponseForFileHeader(httpStatusCode);
                });
            _client.SetCustomWebRequest(mockWebRequest.Object);

            // Act
            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

            // Assert
            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(HttpStatusCode.NotFound, 7)]
        [InlineData(HttpStatusCode.Conflict, 0)] // Any error that isn't the above will return ResultStatus.ERROR
        public async Task TestGetFileHeaderAsync(HttpStatusCode httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(client => client.GetResponseAsync())
                .Returns(() =>
                {
                    return Task.FromResult((WebResponse)MockGCSClient.CreateResponseForFileHeader(httpStatusCode));
                });
            _client.SetCustomWebRequest(mockWebRequest.Object);

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
                Assert.Equal(MockGCSClient.ContentLength, fileHeader.contentLength);
                Assert.Equal(MockGCSClient.SFCDigest, fileHeader.digest);
                Assert.Null(_fileMetadata.lastError);
            }
            else if (expectedResultStatus == ResultStatus.NOT_FOUND_FILE)
            {
                Assert.Null(fileHeader);
                Assert.Null(_fileMetadata.lastError);
            }
            else
            {
                Assert.Null(fileHeader);
                Assert.NotNull(_fileMetadata.lastError);
            }
            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 1)]
        [InlineData(HttpStatusCode.BadRequest, 6)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(null, 0)]
        public void TestUploadFile(HttpStatusCode? httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(c => c.Headers).Returns(new WebHeaderCollection());
            mockWebRequest.Setup(client => client.GetResponse())
                .Returns(() => MockGCSClient.CreateResponseForUploadFile(httpStatusCode));
            mockWebRequest.Setup(client => client.GetRequestStream())
                .Returns(() => new MemoryStream());
            _client.SetCustomWebRequest(mockWebRequest.Object);
            _fileMetadata.uploadSize = UploadFileSize;

            // Act
            _client.UploadFile(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockGCSClient.GcsIV,
                key = MockGCSClient.GcsKey,
                matDesc = MockGCSClient.GcsMatdesc
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
        [InlineData(null, 0)]
        public async Task TestUploadFileAsync(HttpStatusCode? httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(c => c.Headers).Returns(new WebHeaderCollection());
            mockWebRequest.Setup(client => client.GetResponseAsync())
                .Returns(() => Task.FromResult((WebResponse)MockGCSClient.CreateResponseForUploadFile(httpStatusCode)));
            mockWebRequest.Setup(client => client.GetRequestStreamAsync())
                .Returns(() => Task.FromResult((Stream)new MemoryStream()));
            _client.SetCustomWebRequest(mockWebRequest.Object);
            _fileMetadata.uploadSize = UploadFileSize;

            // Act
            await _client.UploadFileAsync(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockGCSClient.GcsIV,
                key = MockGCSClient.GcsKey,
                matDesc = MockGCSClient.GcsMatdesc
            },
            _cancellationToken).ConfigureAwait(false);

            // Assert
            AssertForUploadFileTests(expectedResultStatus);
        }

        private void AssertForUploadFileTests(int expectedResultStatusInt)
        {
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.Equal(_fileMetadata.uploadSize, _fileMetadata.destFileSize);
            }

            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(null, 0)]
        public void TestDownloadFile(HttpStatusCode? httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(client => client.GetResponse())
                .Returns(() =>
                {
                    return MockGCSClient.CreateResponseForDownloadFile(httpStatusCode);
                });
            _client.SetCustomWebRequest(mockWebRequest.Object);

            // Act
            _client.DownloadFile(_fileMetadata, t_downloadFileName, Parallel);

            // Assert
            AssertForDownloadFileTests(expectedResultStatus);
        }

        [SFTheory]
        [InlineData(HttpStatusCode.OK, 2)]
        [InlineData(HttpStatusCode.Unauthorized, 5)]
        [InlineData(HttpStatusCode.Forbidden, 8)]
        [InlineData(HttpStatusCode.InternalServerError, 8)]
        [InlineData(HttpStatusCode.ServiceUnavailable, 8)]
        [InlineData(null, 0)]
        public async Task TestDownloadFileAsync(HttpStatusCode? httpStatusCode, int expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(client => client.GetResponseAsync())
                .Returns(() =>
                {
                    return Task.FromResult((WebResponse)MockGCSClient.CreateResponseForDownloadFile(httpStatusCode));
                });
            _client.SetCustomWebRequest(mockWebRequest.Object);

            // Act
            await _client.DownloadFileAsync(_fileMetadata, t_downloadFileName, Parallel, _cancellationToken).ConfigureAwait(false);

            // Assert
            AssertForDownloadFileTests(expectedResultStatus);
        }

        [SFTheory]
        [InlineData("us-central1", null, false, "https://storage.googleapis.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [InlineData("us-central1", "example.com", false, "https://example.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [InlineData("us-central1", "https://example.com", false, "https://example.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [InlineData("us-central1", null, true, "https://storage.us-central1.rep.googleapis.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [InlineData("me-central2", null, false, "https://storage.me-central2.rep.googleapis.com/mock-customer-stage/mock-id/tables/mock-key/")]
        public void TestUseUriWithRegionsWhenNeeded(string region, string endPoint, bool useRegionalUrl, string expectedRequestUri)
        {
            var fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = endPoint,
                    location = Location,
                    locationType = SFRemoteStorageUtil.GCS_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = region,
                    stageCredentials = _stageCredentials,
                    storageAccount = null,
                    useRegionalUrl = useRegionalUrl
                }
            };

            // act
            var uri = _client.FormBaseRequest(fileMetadata, "PUT").RequestUri.ToString();

            // assert
            Assert.Equal(expectedRequestUri, uri);
        }

        [SFTheory]
        [InlineData("mock-stage", null, false, true, "https://mock-stage.storage.googleapis.com/")]
        [InlineData("mock-stage/mock-id/mock-key", null, false, true, "https://mock-stage.storage.googleapis.com/mock-id/mock-key/")]
        [InlineData("mock-stage/mock-id/mock-key", null, true, true, "https://mock-stage.storage.googleapis.com/mock-id/mock-key/")]
        [InlineData("mock-stage/mock-id/mock-key", "https://example.com", true, true, "https://example.com/mock-id/mock-key/")]
        public void TestUsesVirtualUrlWhenExpected(string location, string endPoint, bool useRegionalUrl, bool useVirtualUrl, string expectedRequestUri)
        {
            var fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = endPoint,
                    location = location,
                    locationType = SFRemoteStorageUtil.GCS_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = null,
                    stageCredentials = _stageCredentials,
                    storageAccount = null,
                    useRegionalUrl = useRegionalUrl,
                    useVirtualUrl = useVirtualUrl
                }
            };

            // act
            var uri = _client.FormBaseRequest(fileMetadata, "PUT").RequestUri.ToString();

            // assert
            Assert.Equal(expectedRequestUri, uri);
        }

        [SFTheory]
        [InlineData("some-header-name", "SOME-HEADER-NAME")]
        [InlineData("SOME-HEADER-NAME", "some-header-name")]
        public void TestGcsHeadersAreCaseInsensitiveForHttpResponseMessage(string headerNameToAdd, string headerNameToGet)
        {
            // arrange
            const string HeaderValue = "someValue";
            var responseMessage = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("Response content") };
            responseMessage.Headers.Add(headerNameToAdd, HeaderValue);

            // act
            var header = responseMessage.Headers.GetValues(headerNameToGet);

            // assert
            Assert.NotNull(header);
            Assert.Single(header);
            Assert.Equal(HeaderValue, header.First());
        }

        [SFTheory]
        [InlineData("some-header-name", "SOME-HEADER-NAME")]
        [InlineData("SOME-HEADER-NAME", "some-header-name")]
        public void TestGcsHeadersAreCaseInsensitiveForWebHeaderCollection(string headerNameToAdd, string headerNameToGet)
        {
            // arrange
            const string HeaderValue = "someValue";
            var headers = new WebHeaderCollection();
            headers.Add(headerNameToAdd, HeaderValue);

            // act
            var header = headers.GetValues(headerNameToGet);

            // assert
            Assert.NotNull(header);
            Assert.Single(header);
            Assert.Equal(HeaderValue, header.First());
        }

        [SFFact]
        public void TestHandleGetFileHeaderResponseWithoutSfcDigest()
        {
            // arrange
            var headers = new WebHeaderCollection();
            headers.Add("content-length", "123");
            var stageInfo = new PutGetStageInfo() { stageCredentials = new Dictionary<string, string>() };
            var client = new SFGCSClient(stageInfo);
            var response = new Mock<HttpWebResponse>();
            response.Setup(r => r.Headers).Returns(headers);
            var fileMetadata = new SFFileMetadata();

            // act
            var fileHeader = client.handleGetFileHeaderResponse(response.Object, fileMetadata);

            // assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), fileMetadata.resultStatus);
            Assert.Null(fileHeader.digest);
            Assert.Equal(123, fileHeader.contentLength);
        }

        private void AssertForDownloadFileTests(int expectedResultStatusInt)
        {
            var expectedResultStatus = (ResultStatus)expectedResultStatusInt;
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(t_downloadFileName);
                Assert.Equal(MockGCSClient.GcsFileContent, text);
                File.Delete(t_downloadFileName);
            }

            Assert.Equal(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
