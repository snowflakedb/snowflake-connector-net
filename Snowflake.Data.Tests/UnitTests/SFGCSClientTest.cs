/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

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
        const string DownloadFileName = "mockFileName.txt";

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
            MockGCSHttpClient mockClient = new MockGCSHttpClient();
            _client = new SFGCSClient(_fileMetadata.stageInfo, mockClient);

            _cancellationToken = new CancellationToken();
        }

        [Test]
        [Ignore("GCSClientTest")]
        public void GCSClientTestDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestConstructorWithoutGCSAccessToken()
        {
            _fileMetadata.stageInfo.stageCredentials = new Dictionary<string, string>();
            new SFGCSClient(_fileMetadata.stageInfo);
        }

        [Test]
        public void TestExtractBucketNameAndPath()
        {
            RemoteLocation location = _client.ExtractBucketNameAndPath(_fileMetadata.stageInfo.location);

            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = Location.Split(new[] { '/' }, 2);

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
            _fileMetadata.sha256Digest = MockGCSHttpClient.SFCDigest;
            _fileMetadata.srcFileSize = MockGCSHttpClient.ContentLength;
            _fileMetadata.encryptionMetadata = new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GcsIV,
                key = MockGCSHttpClient.GcsKey,
                matDesc = MockGCSHttpClient.GcsMatdesc
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
            _fileMetadata.sha256Digest = MockGCSHttpClient.SFCDigest;
            _fileMetadata.srcFileSize = MockGCSHttpClient.ContentLength;
            _fileMetadata.encryptionMetadata = new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GcsIV,
                key = MockGCSHttpClient.GcsKey,
                matDesc = MockGCSHttpClient.GcsMatdesc
            };

            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            Assert.AreEqual(MockGCSHttpClient.ContentLength, fileHeader.contentLength);
            Assert.AreEqual(MockGCSHttpClient.SFCDigest, fileHeader.digest);
            Assert.AreEqual(MockGCSHttpClient.GcsIV, fileHeader.encryptionMetadata.iv);
            Assert.AreEqual(MockGCSHttpClient.GcsKey, fileHeader.encryptionMetadata.key);
            Assert.AreEqual(MockGCSHttpClient.GcsMatdesc, fileHeader.encryptionMetadata.matDesc);
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
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

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
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            CancellationToken cancellationToken = new CancellationToken();
            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockGCSHttpClient.ContentLength, fileHeader.contentLength);
                Assert.AreEqual(MockGCSHttpClient.SFCDigest, fileHeader.digest);
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
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();
            _fileMetadata.uploadSize = UploadFileSize;

            _client.UploadFile(_fileMetadata, new byte[0], new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GcsIV,
                key = MockGCSHttpClient.GcsKey,
                matDesc = MockGCSHttpClient.GcsMatdesc
            });

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
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();
            _fileMetadata.uploadSize = UploadFileSize;

            await _client.UploadFileAsync(_fileMetadata, new byte[0], new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GcsIV,
                key = MockGCSHttpClient.GcsKey,
                matDesc = MockGCSHttpClient.GcsMatdesc
            },
            _cancellationToken).ConfigureAwait(false);

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
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            _client.DownloadFile(_fileMetadata, DownloadFileName, Parallel);

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
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            await _client.DownloadFileAsync(_fileMetadata, DownloadFileName, Parallel, _cancellationToken).ConfigureAwait(false);

            AssertForDownloadFileTests(expectedResultStatus);
        }

        private void AssertForDownloadFileTests(ResultStatus expectedResultStatus)
        {
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(DownloadFileName);
                Assert.AreEqual(MockGCSHttpClient.GcsFileContent, text);
                File.Delete(DownloadFileName);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
