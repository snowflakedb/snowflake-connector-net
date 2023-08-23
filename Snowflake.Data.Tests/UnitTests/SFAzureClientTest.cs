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

    [TestFixture]
    class SFAzureClientTest : SFBaseTest
    {
        // Mock data for file metadata
        const string ENDPOINT = "blob.core.windows.net";

        const string LOCATION_STAGE = "mock-customer-stage";
        const string LOCATION_ID = "mock-id";
        const string LOCATION_TABLES = "tables";
        const string LOCATION_KEY = "mock-key";
        const string PATH = LOCATION_TABLES + "/" + LOCATION_KEY + "/";
        const string LOCATION = LOCATION_STAGE + "/" + LOCATION_ID + "/" + PATH;

        const string REGION = "canadacentral";

        Dictionary<string, string> STAGE_CREDENTIALS = new Dictionary<string, string>()
        {
            {"AZURE_SAS_TOKEN", "MOCK_AZURE_SAS_TOKEN"}
        };

        const string STORAGE_ACCOUNT = "mockStorageAccount";

        // Settings for mock client
        const int PARALLEL = 0;

        // File name for download tests
        const string DOWNLOAD_FILE_NAME = "mockFileName.txt";

        // Token for async tests
        CancellationToken cancellationToken;

        // Mock upload file size
        const int UPLOAD_FILE_SIZE = 9999;

        // The mock client and metadata
        SFSnowflakeAzureClient client;
        SFFileMetadata fileMetadata;

        [SetUp]
        public void BeforeTest()
        {
            fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = ENDPOINT,
                    isClientSideEncrypted = true,
                    location = LOCATION,
                    locationType = SFRemoteStorageUtil.AZURE_FS,
                    path = PATH,
                    presignedUrl = null,
                    region = REGION,
                    stageCredentials = STAGE_CREDENTIALS,
                    storageAccount = STORAGE_ACCOUNT
                }
            };

            // Setup mock client
            MockAzureClient mockClient = new MockAzureClient();
            client = new SFSnowflakeAzureClient(fileMetadata.stageInfo, mockClient);

            cancellationToken = new CancellationToken();
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
            RemoteLocation location = client.ExtractBucketNameAndPath(fileMetadata.stageInfo.location);

            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = LOCATION.Split(new[] { '/' }, 2);

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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();

            FileHeader fileHeader = client.GetFileHeader(fileMetadata);

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
            fileMetadata.stageInfo.location = httpStatusCode.ToString();

            FileHeader fileHeader = await client.GetFileHeaderAsync(fileMetadata, cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockBlobClient.CONTENT_LENGTH, fileHeader.contentLength);
                Assert.AreEqual(MockBlobClient.SFC_DIGEST, fileHeader.digest);
                Assert.AreEqual(MockBlobClient.AZURE_IV, fileHeader.encryptionMetadata.iv);
                Assert.AreEqual(MockBlobClient.AZURE_KEY, fileHeader.encryptionMetadata.key);
                Assert.AreEqual(MockBlobClient.AZURE_MATDESC, fileHeader.encryptionMetadata.matDesc);
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
                iv = MockBlobClient.AZURE_IV,
                key = MockBlobClient.AZURE_KEY,
                matDesc = MockBlobClient.AZURE_MATDESC
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
                iv = MockBlobClient.AZURE_IV,
                key = MockBlobClient.AZURE_KEY,
                matDesc = MockBlobClient.AZURE_MATDESC
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
            Assert.AreEqual(expectedResultStatus.ToString(), fileMetadata.resultStatus);
        }
    }
}
