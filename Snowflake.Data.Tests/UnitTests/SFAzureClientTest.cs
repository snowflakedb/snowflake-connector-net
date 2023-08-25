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
    using Snowflake.Data.Tests.Mock;
    using System.Threading;
    using System.Threading.Tasks;
    using System.IO;

    [TestFixture]
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
        const string DownloadFileName = "mockFileName.txt";

        // Token for async tests
        CancellationToken _cancellationToken;

        // Mock upload file size
        const int UploadFileSize = 9999;

        // The mock client and metadata
        SFSnowflakeAzureClient _client;
        SFFileMetadata _fileMetadata;

        [SetUp]
        public void BeforeTest()
        {
            _fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = EndPoint,
                    isClientSideEncrypted = true,
                    location = Location,
                    locationType = SFRemoteStorageUtil.AZURE_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = Region,
                    stageCredentials = _stageCredentials,
                    storageAccount = StorageAccount
                }
            };

            // Setup mock client
            MockAzureBlobClient blobServiceClientMock = new MockAzureBlobClient();
            _client = new SFSnowflakeAzureClient(_fileMetadata.stageInfo, blobServiceClientMock);

            _cancellationToken = new CancellationToken();
        }

        [Test]
        [Ignore("AzureClientTest")]
        public void AzureClientTestDone()
        {
            // Do nothing;
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
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.ERROR)]  // Any error that isn't the above will return ResultStatus.ERROR
        public void TestGetFileHeader(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        [Test]
        [TestCase(HttpStatusCode.OK, ResultStatus.UPLOADED)]
        [TestCase(HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(HttpStatusCode.Forbidden, ResultStatus.ERROR)]  // Any error that isn't the above will return ResultStatus.ERROR
        public async Task TestGetFileHeaderAsync(HttpStatusCode httpStatusCode, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = httpStatusCode.ToString();

            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockBlobClient.ContentLength, fileHeader.contentLength);
                Assert.AreEqual(MockBlobClient.SFCDigest, fileHeader.digest);
                Assert.AreEqual(MockBlobClient.AzureIV, fileHeader.encryptionMetadata.iv);
                Assert.AreEqual(MockBlobClient.AzureKey, fileHeader.encryptionMetadata.key);
                Assert.AreEqual(MockBlobClient.AzureMatdesc, fileHeader.encryptionMetadata.matDesc);
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

            _client.UploadFile(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockBlobClient.AzureIV,
                key = MockBlobClient.AzureKey,
                matDesc = MockBlobClient.AzureMatdesc
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

            await _client.UploadFileAsync(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockBlobClient.AzureIV,
                key = MockBlobClient.AzureKey,
                matDesc = MockBlobClient.AzureMatdesc
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
            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
