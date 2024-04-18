/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Amazon.S3.Encryption;

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
    using Moq;
    using Amazon.S3.Model;

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
        [ThreadStatic] private static string t_downloadFileName;

        // Token for async tests
        CancellationToken _cancellationToken;

        // Mock upload file size
        const int UploadFileSize = 9999;

        // The mock client and metadata
        SFS3Client _client;
        SFFileMetadata _fileMetadata;
        AmazonS3Config _clientConfig;

        [SetUp]
        public void BeforeTest()
        {
            t_downloadFileName = TestNameWithWorker + "_mockFileName.txt";
            
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

            _clientConfig = new AmazonS3Config();
            _clientConfig.RegionEndpoint = RegionEndpoint.GetBySystemName(_fileMetadata.stageInfo.region);

            _cancellationToken = new CancellationToken();
        }

        [Test]
        public void TestExtractBucketNameAndPath()
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            // Split LOCATION based on the first '/' character
            string[] bucketAndKey = Location.Split(new[] { '/' }, 2);

            // Act
            var location = _client.ExtractBucketNameAndPath(_fileMetadata.stageInfo.location);

            // Assert
            Assert.AreEqual(bucketAndKey[0], location.bucket);
            Assert.AreEqual(bucketAndKey[1], location.key);
        }

        [Test]
        [TestCase(MockS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(SFS3Client.NO_SUCH_KEY, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(MockS3Client.AwsStatusError, ResultStatus.ERROR)] // Any error that isn't the above will return ResultStatus.ERROR
        public void TestGetFileHeader(string requestKey, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            mockAmazonS3Client.Setup(client => client.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns<GetObjectRequest, CancellationToken>((request, cancellationToken) =>
                {
                    return MockS3Client.CreateResponseForGetFileHeader(request.BucketName, false);
                });
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            _fileMetadata.stageInfo.location = requestKey;

            // Act
            FileHeader fileHeader = _client.GetFileHeader(_fileMetadata);

            // Assert
            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        [Test]
        [TestCase(MockS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(SFS3Client.NO_SUCH_KEY, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(MockS3Client.AwsStatusError, ResultStatus.ERROR)] // Any error that isn't the above will return ResultStatus.ERROR
        public async Task TestGetFileHeaderAsync(string requestKey, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            mockAmazonS3Client.Setup(client => client.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns<GetObjectRequest, CancellationToken>((request, cancellationToken) =>
                {
                    return MockS3Client.CreateResponseForGetFileHeader(request.BucketName, true);
                });
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            _fileMetadata.stageInfo.location = requestKey;

            // Act
            FileHeader fileHeader = await _client.GetFileHeaderAsync(_fileMetadata, _cancellationToken).ConfigureAwait(false);

            // Assert
            AssertForGetFileHeaderTests(expectedResultStatus, fileHeader);
        }

        private void AssertForGetFileHeaderTests(ResultStatus expectedResultStatus, FileHeader fileHeader)
        {
            if (expectedResultStatus == ResultStatus.UPLOADED)
            {
                Assert.AreEqual(MockS3Client.ContentLength, fileHeader.contentLength);
                Assert.AreEqual(MockS3Client.SfcDigest, fileHeader.digest);
                Assert.AreEqual(MockS3Client.AmzIV, fileHeader.encryptionMetadata.iv);
                Assert.AreEqual(MockS3Client.AmzKey, fileHeader.encryptionMetadata.key);
                Assert.AreEqual(MockS3Client.AmzMatdesc, fileHeader.encryptionMetadata.matDesc);
            }
            else
            {
                Assert.IsNull(fileHeader);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }

        [Test]
        [TestCase(MockS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public void TestUploadFile(string requestKey, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            mockAmazonS3Client.Setup(client => client.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns<PutObjectRequest, CancellationToken>((request, cancellationToken) =>
                {
                    return MockS3Client.CreateResponseForUploadFile(request.BucketName, false);
                });
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            _fileMetadata.stageInfo.location = requestKey;
            _fileMetadata.uploadSize = UploadFileSize;

            // Act
            _client.UploadFile(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockS3Client.AmzIV,
                key = MockS3Client.AmzKey,
                matDesc = MockS3Client.AmzMatdesc
            });

            // Assert
            AssertForUploadFileTests(expectedResultStatus);
        }

        [Test]
        public void TestAppendHttpsToEndpoint()
        {
            // Arrange
            var amazonS3Client = new AmazonS3Config();
            var endpoint = "endpointWithNoHttps.com";
            var expectedEndpoint = "https://endpointWithNoHttps.com";

            // ACT
            SFS3Client.SetCommonClientConfig(amazonS3Client, string.Empty, endpoint, 1, 0);

            // Assert
            Assert.That(amazonS3Client.ServiceURL, Is.EqualTo(expectedEndpoint));
        }

        [Test]
        public void TestAppendHttpsToEndpointWithBrackets()
        {
            // Arrange
            var amazonS3Client = new AmazonS3Config();
            var endpoint = "[endpointWithNoHttps.com]";
            var expectedEndpoint = "https://endpointWithNoHttps.com";

            // ACT
            SFS3Client.SetCommonClientConfig(amazonS3Client, string.Empty, endpoint, 1, 0);

            // Assert
            Assert.That(amazonS3Client.ServiceURL, Is.EqualTo(expectedEndpoint));
        }

        [Test]
        [TestCase(MockS3Client.AwsStatusOk, ResultStatus.UPLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public async Task TestUploadFileAsync(string requestKey, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            mockAmazonS3Client.Setup(client => client.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns<PutObjectRequest, CancellationToken>((request, cancellationToken) =>
                {
                    return MockS3Client.CreateResponseForUploadFile(request.BucketName, true);
                });
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            _fileMetadata.stageInfo.location = requestKey;
            _fileMetadata.uploadSize = UploadFileSize;

            // Act
            await _client.UploadFileAsync(_fileMetadata, new MemoryStream(), new SFEncryptionMetadata()
            {
                iv = MockS3Client.AmzIV,
                key = MockS3Client.AmzKey,
                matDesc = MockS3Client.AmzMatdesc
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
        [TestCase(MockS3Client.AwsStatusOk, ResultStatus.DOWNLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public void TestDownloadFile(string requestKey, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            mockAmazonS3Client.Setup(client => client.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns<GetObjectRequest, CancellationToken>((request, cancellationToken) =>
                {
                    return MockS3Client.CreateResponseForDownloadFile(request.BucketName, false);
                });
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            _fileMetadata.stageInfo.location = requestKey;

            // Act
            _client.DownloadFile(_fileMetadata, t_downloadFileName, Parallel);

            // Assert
            AssertForDownloadFileTests(expectedResultStatus);
        }

        [Test]
        [TestCase(MockS3Client.AwsStatusOk, ResultStatus.DOWNLOADED)]
        [TestCase(SFS3Client.EXPIRED_TOKEN, ResultStatus.RENEW_TOKEN)]
        [TestCase(MockS3Client.AwsStatusError, ResultStatus.NEED_RETRY)] // Any error that isn't the above will return ResultStatus.NEED_RETRY
        public async Task TestDownloadFileAsync(string requestKey, ResultStatus expectedResultStatus)
        {
            // Arrange
            var mockAmazonS3Client = new Mock<AmazonS3Client>(AwsKeyId, AwsSecretKey, AwsToken, _clientConfig);
            mockAmazonS3Client.Setup(client => client.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns<GetObjectRequest, CancellationToken>((request, cancellationToken) =>
                {
                    return MockS3Client.CreateResponseForDownloadFile(request.BucketName, true);
                });
            _client = new SFS3Client(_fileMetadata.stageInfo, MaxRetry, Parallel, _proxyCredentials, mockAmazonS3Client.Object);
            _fileMetadata.client = _client;
            _fileMetadata.stageInfo.location = requestKey;
            
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
                Assert.AreEqual(MockS3Client.S3FileContent, text);
                File.Delete(t_downloadFileName);
            }

            Assert.AreEqual(expectedResultStatus.ToString(), _fileMetadata.resultStatus);
        }
    }
}
