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
    using Amazon.S3;
    using Snowflake.Data.Tests.Mock;
    using System.Threading.Tasks;
    using Amazon;
    using System.Threading;
    using System.IO;
    using System.Net.Http;

    [TestFixture]
    class SFS3ClientTest : SFBaseTest
    {
        // Mock data for file metadata
        const string Endpoint = "[www.mockEndPoint.com]";

        const string AwsKeyId = "MOCK_AWS_KEY_ID";
        const string AwsSecretKey = "MOCK_AWS_SECRET_KEY";
        const string AwsToken = "MOCK_AWS_TOKEN";
        const string AwsID = "MOCK_AWS_ID";
        const string AwsKey = "MOCK_AWS_KEY";

        const string LocationStage = "mock-customer-stage";
        const string LocationId = "mock-id";
        const string LocationTables = "tables";
        const string LocationKey = "mock-key";
        const string LocationPath = LocationTables + "/" + LocationKey + "/";
        const string Location = LocationStage + "/" + LocationId + "/" + LocationPath;

        const string Region = "us-west-2";

        Dictionary<string, string> _stageCredentials = new Dictionary<string, string>()
        {
            {"AWS_KEY_ID", AwsKeyId},
            {"AWS_SECRET_KEY", AwsSecretKey},
            {"AWS_TOKEN", AwsToken},
            {"AWS_ID", AwsID},
            {"AWS_KEY", AwsKey}
        };

        ProxyCredentials _proxyCredentials = new ProxyCredentials()
        {
            ProxyHost = "proxyHost",
            ProxyPort = 8080,
            ProxyUser = "proxyUser",
            ProxyPassword = "proxyPassword"
        };

        // Settings for mock client
        const int MaxRetry = 0;
        const int Parallel = 0;

        // File name for download tests
        const string DownloadFileName = "mockFileName.txt";

        // Token for async tests
        CancellationToken _cancellationToken;

        // Mock upload file size
        const int UploadFileSize = 9999;

        // The mock client and metadata
        SFS3Client _client;
        SFFileMetadata _fileMetadata;

        [SetUp]
        public void BeforeTest()
        {
            _fileMetadata = new SFFileMetadata()
            {
                stageInfo = new PutGetStageInfo()
                {
                    endPoint = Endpoint,
                    isClientSideEncrypted = true,
                    location = Location,
                    locationType = SFRemoteStorageUtil.S3_FS,
                    path = LocationPath,
                    presignedUrl = null,
                    region = Region,
                    stageCredentials = _stageCredentials,
                    storageAccount = null
                }
            };

            // Setup mock S3 client
            var mockClient = SetUpMockAwsClient(_fileMetadata);
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockClient); 
            _fileMetadata.client = _client;

            _cancellationToken = new CancellationToken();
        }

        internal static MockAmazonS3Client SetUpMockAwsClient(SFFileMetadata fileMetadata)
        {
            var clientConfig = new AmazonS3Config();
            RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(fileMetadata.stageInfo.region);
            clientConfig.RegionEndpoint = regionEndpoint;
            MockAmazonS3Client mockClient = new MockAmazonS3Client(AwsKeyId,
                AwsSecretKey,
                AwsToken,
                clientConfig);

            return mockClient;
        }

        [Test]
        [Ignore("S3ClientTest")]
        public void S3ClientTestDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestExtractBucketNameAndPath()
        {
            var location = _client.ExtractBucketNameAndPath(_fileMetadata.stageInfo.location);

            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = Location.Split(new[] { '/' }, 2);

            Assert.AreEqual(bucketAndKey[0], location.bucket);
            Assert.AreEqual(bucketAndKey[1], location.key);
        }

        [Test]
        [TestCase(MockAmazonS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(SFS3Client.NO_SUCH_KEY, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(MockAmazonS3Client.AwsStatusError, ResultStatus.ERROR)] // Any error that isn't the above will return ResultStatus.ERROR
        public void TestGetFileHeader(string requestKey, ResultStatus expectedResultStatus)
        {
            // arrange request
            _fileMetadata.stageInfo.location = requestKey + "/" + HttpMethod.Head;

            // act
            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

            // assert
            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        [Test]
        [TestCase(MockAmazonS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(SFS3Client.NO_SUCH_KEY, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(MockAmazonS3Client.AwsStatusError, ResultStatus.ERROR)] // Any error that isn't the above will return ResultStatus.ERROR
        public async Task TestGetFileHeaderAsync(string requestKey, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = requestKey + "/" + HttpMethod.Head + "/async";

            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockAmazonS3Client.ContentLength, fileHeader.contentLength);
                Assert.AreEqual(MockAmazonS3Client.SfcDigest, fileHeader.digest);
                Assert.AreEqual(MockAmazonS3Client.AmzIV, fileHeader.encryptionMetadata.iv);
                Assert.AreEqual(MockAmazonS3Client.AmzKey, fileHeader.encryptionMetadata.key);
                Assert.AreEqual(MockAmazonS3Client.AmzMatdesc, fileHeader.encryptionMetadata.matDesc);
            }
            else
            {
                Assert.IsNull(fileHeader);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(MockAmazonS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockAmazonS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public void TestUploadFile(string requestKey, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = requestKey + "/" + HttpMethod.Put;
            _fileMetadata.uploadSize = UploadFileSize;

            _client.UploadFile(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockAmazonS3Client.AmzIV,
                key = MockAmazonS3Client.AmzKey,
                matDesc = MockAmazonS3Client.AmzMatdesc
            });

            AssertForUploadFileTests(expectedResultStatus);
        }

        [Test]
        [TestCase(MockAmazonS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockAmazonS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public async Task TestUploadFileAsync(string requestKey, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = requestKey + "/" + HttpMethod.Put + "/async";
            _fileMetadata.uploadSize = UploadFileSize;

            await _client.UploadFileAsync(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockAmazonS3Client.AmzIV,
                key = MockAmazonS3Client.AmzKey,
                matDesc = MockAmazonS3Client.AmzMatdesc
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
        [TestCase(MockAmazonS3Client.AwsStatusOk, ResultStatus.DOWNLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockAmazonS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public void TestDownloadFile(string requestKey, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = requestKey + "/" + HttpMethod.Get;

            _client.DownloadFile(_fileMetadata, DownloadFileName, Parallel);

            AssertForDownloadFileTests(expectedResultStatus);
        }

        [Test]
        [TestCase(MockAmazonS3Client.AwsStatusOk, ResultStatus.DOWNLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockAmazonS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public async Task TestDownloadFileAsync(string requestKey, ResultStatus expectedResultStatus)
        {
            // Setup request
            _fileMetadata.stageInfo.location = requestKey + "/" + HttpMethod.Get + "/async";
            
            await _client.DownloadFileAsync(_fileMetadata, DownloadFileName, Parallel, _cancellationToken).ConfigureAwait(false);

            AssertForDownloadFileTests(expectedResultStatus);
        }

        private void AssertForDownloadFileTests(ResultStatus expectedResultStatus)
        {
            if (expectedResultStatus == ResultStatus.DOWNLOADED)
            {
                string text = File.ReadAllText(DownloadFileName);
                Assert.AreEqual(MockAmazonS3Client.S3FileContent, text);
                File.Delete(DownloadFileName);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
