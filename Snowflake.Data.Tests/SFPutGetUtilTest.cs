/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using RichardSzalay.MockHttp;
    using System.Threading.Tasks;
    using System.Net;
    using Snowflake.Data.Core.FileTransfer.StorageClient;
    using System;
    using System.Collections.Generic;

    [TestFixture]
    class SFPutGetUtilTest
    {
        private const string S3 = "S3";
        private const string AZURE = "AZURE";
        private const string GCS = "GCS";

        [Test]
        [TestCase(S3, HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(S3, HttpStatusCode.Unauthorized, ResultStatus.ERROR)]
        [TestCase(S3, HttpStatusCode.Forbidden, ResultStatus.ERROR)]
        [TestCase(S3, HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(S3, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(S3, HttpStatusCode.InternalServerError, ResultStatus.ERROR)]
        [TestCase(S3, HttpStatusCode.ServiceUnavailable, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(AZURE, HttpStatusCode.Unauthorized, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.Forbidden, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(AZURE, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.InternalServerError, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.ServiceUnavailable, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.BadRequest, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.Unauthorized, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(GCS, HttpStatusCode.Forbidden, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(GCS, HttpStatusCode.NotFound, ResultStatus.NOT_FOUND_FILE)]
        [TestCase(GCS, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.InternalServerError, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.ServiceUnavailable, ResultStatus.ERROR)]
        public async Task TestHandleFileHeaderErr(string clientType, HttpStatusCode statusCode, ResultStatus expectedResultStatus)
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("https://test.snowflakecomputing.com")
            .Respond(statusCode);
            var client = mockHttp.ToHttpClient();
            await client.GetAsync("https://test.snowflakecomputing.com");

            ResultStatus resultStatus = ResultStatus.ERROR;
            if (clientType == S3)
            {
                SFS3Client s3 = new SFS3Client();
                resultStatus = s3.HandleFileHeaderErr(statusCode);
            }
            else if (clientType == AZURE)
            {
                SFSnowflakeAzureClient azure = new SFSnowflakeAzureClient();
                resultStatus = azure.HandleFileHeaderErr(statusCode);
            }
            else if (clientType == GCS)
            {
                PutGetStageInfo stageInfo = new PutGetStageInfo()
                {
                    stageCredentials = new Dictionary<string, string>()
                };
                SFGCSClient gcs = new SFGCSClient(stageInfo);
                resultStatus = gcs.HandleFileHeaderErr(statusCode);
            }

            Assert.AreEqual(expectedResultStatus, resultStatus);
        }

        [Test]
        [TestCase(S3, HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(S3, HttpStatusCode.Unauthorized, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.NotFound, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.RequestTimeout, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(AZURE, HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(AZURE, HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(AZURE, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(AZURE, HttpStatusCode.NotFound, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(AZURE, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(GCS, HttpStatusCode.BadRequest, ResultStatus.RENEW_PRESIGNED_URL)]
        [TestCase(GCS, HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(GCS, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(GCS, HttpStatusCode.NotFound, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(GCS, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public async Task TestHandleFileUploadErr(string clientType, HttpStatusCode statusCode, ResultStatus expectedResultStatus)
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("https://test.snowflakecomputing.com")
            .Respond(statusCode);
            var client = mockHttp.ToHttpClient();
            await client.GetAsync("https://test.snowflakecomputing.com");

            ResultStatus resultStatus = ResultStatus.ERROR;
            if (clientType == S3)
            {
                SFS3Client s3 = new SFS3Client();
                resultStatus = s3.HandleFileLoadErr(statusCode);
            }
            else if (clientType == AZURE)
            {
                SFSnowflakeAzureClient azure = new SFSnowflakeAzureClient();
                resultStatus = azure.HandleUploadFileErr(statusCode);
            }
            else if (clientType == GCS)
            {
                PutGetStageInfo stageInfo = new PutGetStageInfo()
                {
                    stageCredentials = new Dictionary<string, string>()
                };
                SFGCSClient gcs = new SFGCSClient(stageInfo);
                resultStatus = gcs.HandleUploadFileErr(statusCode);
            }

            Assert.AreEqual(expectedResultStatus, resultStatus);
        }

        [Test]
        [TestCase(S3, HttpStatusCode.BadRequest, ResultStatus.RENEW_TOKEN)]
        [TestCase(S3, HttpStatusCode.Unauthorized, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.NotFound, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.RequestTimeout, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(S3, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(AZURE, HttpStatusCode.BadRequest, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(AZURE, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(AZURE, HttpStatusCode.NotFound, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(AZURE, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(AZURE, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        [TestCase(GCS, HttpStatusCode.BadRequest, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.Unauthorized, ResultStatus.RENEW_TOKEN)]
        [TestCase(GCS, HttpStatusCode.Forbidden, ResultStatus.NEED_RETRY)]
        [TestCase(GCS, HttpStatusCode.NotFound, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.RequestTimeout, ResultStatus.ERROR)]
        [TestCase(GCS, HttpStatusCode.InternalServerError, ResultStatus.NEED_RETRY)]
        [TestCase(GCS, HttpStatusCode.ServiceUnavailable, ResultStatus.NEED_RETRY)]
        public async Task TestHandleFileDownloadErr(string clientType, HttpStatusCode statusCode, ResultStatus expectedResultStatus)
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("https://test.snowflakecomputing.com")
            .Respond(statusCode);
            var client = mockHttp.ToHttpClient();
            await client.GetAsync("https://test.snowflakecomputing.com");

            ResultStatus resultStatus = ResultStatus.ERROR;
            if (clientType == S3)
            {
                SFS3Client s3 = new SFS3Client();
                resultStatus = s3.HandleFileLoadErr(statusCode);
            }
            else if (clientType == AZURE)
            {
                SFSnowflakeAzureClient azure = new SFSnowflakeAzureClient();
                resultStatus = azure.HandleDownloadFileErr(statusCode);
            }
            else if (clientType == GCS)
            {
                PutGetStageInfo stageInfo = new PutGetStageInfo()
                {
                    stageCredentials = new Dictionary<string, string>()
                };
                SFGCSClient gcs = new SFGCSClient(stageInfo);
                resultStatus = gcs.HandleDownloadFileErr(statusCode);
            }

            Assert.AreEqual(expectedResultStatus, resultStatus);
        }
    }
}
