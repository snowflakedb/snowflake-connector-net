/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
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

        const string LOCATION_STAGE = "mock-customer-stage";
        const string LOCATION_ID = "mock-id";
        const string LOCATION_TABLES = "tables";
        const string LOCATION_KEY = "mock-key";
        const string PATH = LOCATION_TABLES + "/" + LOCATION_KEY + "/";
        const string LOCATION = LOCATION_STAGE + "/" + LOCATION_ID + "/" + PATH;

        const string REGION = "US-CENTRAL1";

        Dictionary<string, string> STAGE_CREDENTIALS = new Dictionary<string, string>()
        {
            {"GCS_ACCESS_TOKEN", "MOCK_GCS_ACCESS_TOKEN"}
        };

        // Settings for mock client
        const int PARALLEL = 0;

        // File name for download tests
        const string DOWNLOAD_FILE_NAME = "mockFileName.txt";

        // Token for async tests
        CancellationToken cancellationToken;

        // Mock upload file size
        const int UPLOAD_FILE_SIZE = 9999;

        // The mock client and metadata
        SFGCSClient client;
        SFFileMetadata fileMetadata;

        [SetUp]
        public void BeforeTest()
        {
            fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = null,
                    isClientSideEncrypted = true,
                    location = LOCATION,
                    locationType = SFRemoteStorageUtil.GCS_FS,
                    path = PATH,
                    presignedUrl = null,
                    region = REGION,
                    stageCredentials = STAGE_CREDENTIALS,
                    storageAccount = null
                }
            };

            // Setup mock GCS HTTP client
            MockGCSHttpClient mockClient = new MockGCSHttpClient();
            client = new SFGCSClient(fileMetadata.stageInfo, mockClient);

            cancellationToken = new CancellationToken();
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
            fileMetadata.stageInfo.stageCredentials = new Dictionary<string, string>();
            new SFGCSClient(fileMetadata.stageInfo);
        }

        [Test]
        public void TestExtractBucketNameAndPath()
        {
            RemoteLocation location = client.ExtractBucketNameAndPath(fileMetadata.stageInfo.location);

            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = LOCATION.Split(new[] { '/' }, 2);

            Assert.AreEqual(bucketAndKey[0], location.bucket);
            Assert.AreEqual(bucketAndKey[1], location.key);
        }

        [Test]
        [TestCase(ResultStatus.UPLOADED)]
        [TestCase(ResultStatus.DOWNLOADED)]
        public void TestGetFileHeaderWhenFileHeaderAlreadyExists(ResultStatus expectedResultStatus)
        {
            // Setup file metadata
            fileMetadata.resultStatus = expectedResultStatus.ToString();
            fileMetadata.sha256Digest = MockGCSHttpClient.SFC_DIGEST;
            fileMetadata.srcFileSize = MockGCSHttpClient.CONTENT_LENGTH;
            fileMetadata.encryptionMetadata = new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GCS_IV,
                key = MockGCSHttpClient.GCS_KEY,
                matDesc = MockGCSHttpClient.GCS_MATDESC
            };

            FileHeader fileHeader = client.GetFileHeader(fileMetadata);

            AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(expectedResultStatus, fileHeader);
        }

        [Test]
        [TestCase(ResultStatus.UPLOADED)]
        [TestCase(ResultStatus.DOWNLOADED)]
        public async Task TestGetFileHeaderAsyncWhenFileHeaderAlreadyExists(ResultStatus expectedResultStatus)
        {
            // Setup file metadata
            fileMetadata.resultStatus = expectedResultStatus.ToString();
            fileMetadata.sha256Digest = MockGCSHttpClient.SFC_DIGEST;
            fileMetadata.srcFileSize = MockGCSHttpClient.CONTENT_LENGTH;
            fileMetadata.encryptionMetadata = new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GCS_IV,
                key = MockGCSHttpClient.GCS_KEY,
                matDesc = MockGCSHttpClient.GCS_MATDESC
            };

            FileHeader fileHeader = await client.GetFileHeaderAsync(fileMetadata, cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderWhenFileHeaderAlreadyExistsTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            Assert.AreEqual(MockGCSHttpClient.CONTENT_LENGTH, fileHeader.contentLength);
            Assert.AreEqual(MockGCSHttpClient.SFC_DIGEST, fileHeader.digest);
            Assert.AreEqual(MockGCSHttpClient.GCS_IV, fileHeader.encryptionMetadata.iv);
            Assert.AreEqual(MockGCSHttpClient.GCS_KEY, fileHeader.encryptionMetadata.key);
            Assert.AreEqual(MockGCSHttpClient.GCS_MATDESC, fileHeader.encryptionMetadata.matDesc);
            Assert.AreEqual(expectedResultStatus.ToString(), fileMetadata.resultStatus);
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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();

            FileHeader fileHeader = client.GetFileHeader(fileMetadata);

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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();

            CancellationToken cancellationToken = new CancellationToken();
            FileHeader fileHeader = await client.GetFileHeaderAsync(fileMetadata, cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockGCSHttpClient.CONTENT_LENGTH, fileHeader.contentLength);
                Assert.AreEqual(MockGCSHttpClient.SFC_DIGEST, fileHeader.digest);
            }
            else
            {
                Assert.IsNull(fileHeader);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), fileMetadata.resultStatus);
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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();
            fileMetadata.uploadSize = UPLOAD_FILE_SIZE;

            client.UploadFile(fileMetadata, new byte[0], new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GCS_IV,
                key = MockGCSHttpClient.GCS_KEY,
                matDesc = MockGCSHttpClient.GCS_MATDESC
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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();
            fileMetadata.uploadSize = UPLOAD_FILE_SIZE;

            await client.UploadFileAsync(fileMetadata, new byte[0], new SFEncryptionMetadata()
            {
                iv = MockGCSHttpClient.GCS_IV,
                key = MockGCSHttpClient.GCS_KEY,
                matDesc = MockGCSHttpClient.GCS_MATDESC
            },
            cancellationToken).ConfigureAwait(false);

            AssertForUploadFileTests(expectedResultStatus);
        }

        private void AssertForUploadFileTests(ResultStatus expectedResultStatus)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(fileMetadata.uploadSize, fileMetadata.destFileSize);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), fileMetadata.resultStatus);
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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();

            client.DownloadFile(fileMetadata, DOWNLOAD_FILE_NAME, PARALLEL);

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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();

            await client.DownloadFileAsync(fileMetadata, DOWNLOAD_FILE_NAME, PARALLEL, cancellationToken).ConfigureAwait(false);

            AssertForDownloadFileTests(expectedResultStatus);
        }

        private void AssertForDownloadFileTests(ResultStatus expectedResultStatus)
        {
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(DOWNLOAD_FILE_NAME);
                Assert.AreEqual(MockGCSHttpClient.FILE_CONTENT, text);
                File.Delete(DOWNLOAD_FILE_NAME);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), fileMetadata.resultStatus);
        }
    }
}
