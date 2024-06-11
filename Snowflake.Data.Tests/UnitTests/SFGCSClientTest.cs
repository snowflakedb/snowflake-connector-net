/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer.StorageClient;
    using Snowflake.Data.Core.FileTransfer;
    using System.Collections.Generic;
    using System.Net;
    using System.IO;
    using System.Threading.Tasks;
    using System.Threading;
    using Snowflake.Data.Tests.Mock;
    using Moq;

    [TestFixture]
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
        public void BeforeTest()
        {
            t_downloadFileName = TestNameWithWorker + "_mockFileName.txt";
            
            _fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = null,
                    isClientSideEncrypted = true,
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
        public void TestUploadFile(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(c => c.Headers).Returns(new WebHeaderCollection());
            mockWebRequest.Setup(client => client.GetResponse())
                .Returns(() =>
                {
                    return MockGCSClient.CreateResponseForUploadFile(httpStatusCode);
                });
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
        public async Task TestUploadFileAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockWebRequest = new Mock<WebRequest>();
            mockWebRequest.Setup(c => c.Headers).Returns(new WebHeaderCollection());
            mockWebRequest.Setup(client => client.GetResponseAsync())
                .Returns(() =>
                {
                    return Task.FromResult((WebResponse)MockGCSClient.CreateResponseForUploadFile(httpStatusCode));
                });
            mockWebRequest.Setup(client => client.GetRequestStreamAsync())
                .Returns(() => Task.FromResult((Stream) new MemoryStream()));
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
        public void TestDownloadFile(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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
        public async Task TestDownloadFileAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
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
