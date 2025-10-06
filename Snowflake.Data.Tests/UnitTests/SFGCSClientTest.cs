using System;
using NUnit.Framework;
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

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class SFGCSClientTest : SFBaseTest
    {
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

        [SetUp]
        public new void BeforeTest()
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

        [Test]
        public void TestExtractBucketNameAndPath()
        {
            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = Location.Split(new[] { '/' }, 2);

            RemoteLocation location = _client.ExtractBucketNameAndPath(_fileMetadata.stageInfo.location);

            Assert.AreEqual(bucketAndKey[0], location.bucket);
            Assert.AreEqual(bucketAndKey[1], location.key);
        }

        [Test]
        [TestCase(ResultStatus.UPLOADED)]
        [TestCase(ResultStatus.DOWNLOADED)]
        public void TestGetFileHeaderWhenFileHeaderAlreadyExists(ResultStatus expectedResultStatus)
        {
            // Setup file metadata
            _fileMetadata.resultStatus = expectedResultStatus.ToString();
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

        [Test]
        [TestCase(ResultStatus.UPLOADED)]
        [TestCase(ResultStatus.DOWNLOADED)]
        public async Task TestGetFileHeaderAsyncWhenFileHeaderAlreadyExists(ResultStatus expectedResultStatus)
        {
            // Setup file metadata
            _fileMetadata.resultStatus = expectedResultStatus.ToString();
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

        private void AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            Assert.AreEqual(MockGCSClient.ContentLength, fileHeader.contentLength);
            Assert.AreEqual(MockGCSClient.SFCDigest, fileHeader.digest);
            Assert.AreEqual(MockGCSClient.GcsIV, fileHeader.encryptionMetadata.iv);
            Assert.AreEqual(MockGCSClient.GcsKey, fileHeader.encryptionMetadata.key);
            Assert.AreEqual(MockGCSClient.GcsMatdesc, fileHeader.encryptionMetadata.matDesc);
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(HttpStatusCode.Conflict, ResultStatus.ERROR)] // Any error that isn't the above will return ResultStatus.ERROR
        public void TestGetFileHeader(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(HttpStatusCode.Conflict, ResultStatus.ERROR)] // Any error that isn't the above will return ResultStatus.ERROR
        public async Task TestGetFileHeaderAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockGCSClient.ContentLength, fileHeader.contentLength);
                Assert.AreEqual(MockGCSClient.SFCDigest, fileHeader.digest);
                Assert.IsNull(_fileMetadata.lastError);
            }
            else if (expectedResultStatus == ResultStatus.NOT_FOUND_FILE)
            {
                Assert.IsNull(fileHeader);
                Assert.IsNull(_fileMetadata.lastError);
            }
            else
            {
                Assert.IsNull(fileHeader);
                Assert.IsNotNull(_fileMetadata.lastError);
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
        [TestCase(null, ResultStatus.ERROR)]
        public void TestUploadFile(HttpStatusCode? httpStatusCode, ResultStatus expectedResultStatus)
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

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(null, ResultStatus.ERROR)]
        public async Task TestUploadFileAsync(HttpStatusCode? httpStatusCode, ResultStatus expectedResultStatus)
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
        [TestCase(null, ResultStatus.ERROR)]
        public void TestDownloadFile(HttpStatusCode? httpStatusCode, ResultStatus expectedResultStatus)
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

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.DOWNLOADED)]
        [TestCase(HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(null, ResultStatus.ERROR)]
        public async Task TestDownloadFileAsync(HttpStatusCode? httpStatusCode, ResultStatus expectedResultStatus)
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

        [Test]
        [TestCase("us-central1", null, null, "https://storage.googleapis.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [TestCase("us-central1", "example.com", null, "https://example.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [TestCase("us-central1", "https://example.com", null, "https://example.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [TestCase("us-central1", null, true, "https://storage.us-central1.rep.googleapis.com/mock-customer-stage/mock-id/tables/mock-key/")]
        [TestCase("me-central2", null, null, "https://storage.me-central2.rep.googleapis.com/mock-customer-stage/mock-id/tables/mock-key/")]
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
            Assert.AreEqual(expectedRequestUri, uri);
        }

        [Test]
        [TestCase("mock-stage", null, false, true, "https://mock-stage.storage.googleapis.com/")]
        [TestCase("mock-stage/mock-id/mock-key", null, false, true, "https://mock-stage.storage.googleapis.com/mock-id/mock-key/")]
        [TestCase("mock-stage/mock-id/mock-key", null, true, true, "https://mock-stage.storage.googleapis.com/mock-id/mock-key/")]
        [TestCase("mock-stage/mock-id/mock-key", "https://example.com", true, true, "https://example.com/mock-id/mock-key/")]
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
            Assert.AreEqual(expectedRequestUri, uri);
        }

        [Test]
        [TestCase("some-header-name", "SOME-HEADER-NAME")]
        [TestCase("SOME-HEADER-NAME", "some-header-name")]
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
            Assert.AreEqual(1, header.Count());
            Assert.AreEqual(HeaderValue, header.First());
        }

        [Test]
        [TestCase("some-header-name", "SOME-HEADER-NAME")]
        [TestCase("SOME-HEADER-NAME", "some-header-name")]
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
            Assert.AreEqual(1, header.Count());
            Assert.AreEqual(HeaderValue, header.First());
        }

        [Test]
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
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), fileMetadata.resultStatus);
            Assert.IsNull(fileHeader.digest);
            Assert.AreEqual(123, fileHeader.contentLength);
        }

        private void AssertForDownloadFileTests(ResultStatus expectedResultStatus)
        {
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(t_downloadFileName);
                Assert.AreEqual(MockGCSClient.GcsFileContent, text);
                File.Delete(t_downloadFileName);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
