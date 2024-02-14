/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Amazon.S3;
using Amazon.S3.Model;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{
    class MockS3Client
    {
        // Mock status codes for S3 requests
        internal const string AwsStatusOk = "OK";
        internal const string AwsStatusError = "AWS_ERROR";

        // Mock S3 data for FileHeader
        internal const string AmzIV = "MOCK_AMZ_IV";
        internal const string AmzKey = "MOCK_AMZ_KEY";
        internal const string AmzMatdesc = "MOCK_AMZ_MATDESC";
        internal const string SfcDigest = "MOCK_SFC_DIGEST";

        // Mock error message for S3 errors
        internal const string S3ErrorMessage = "S3 Error Message";

        // Mock data for downloaded file
        internal const string S3FileContent = "S3ClientTest";

        // Mock content length
        internal const int ContentLength = 9999;

        // Create AWS exception for mock requests
        static Exception CreateMockAwsResponseError(string errorCode, bool isAsync)
        {
            AmazonS3Exception awsError = new AmazonS3Exception(S3ErrorMessage);
            awsError.ErrorCode = errorCode;

            if (isAsync)
            {
                return awsError; // S3 throws the AmazonS3Exception on async calls
            }

            Exception exceptionContainingS3Error = new Exception(S3ErrorMessage, awsError);
            return exceptionContainingS3Error;  // S3 places the AmazonS3Exception on the InnerException property on non-async calls
        }

        // Create mock response for GetFileHeader
        static internal Task<GetObjectResponse> CreateResponseForGetFileHeader(string statusCode, bool isAsync)
        {
            if (statusCode == HttpStatusCode.OK.ToString())
            {
                var getObjectResponse = new GetObjectResponse();
                getObjectResponse.ContentLength = MockS3Client.ContentLength;
                getObjectResponse.Metadata.Add(SFS3Client.AMZ_IV, MockS3Client.AmzIV);
                getObjectResponse.Metadata.Add(SFS3Client.AMZ_KEY, MockS3Client.AmzKey);
                getObjectResponse.Metadata.Add(SFS3Client.AMZ_MATDESC, MockS3Client.AmzMatdesc);
                getObjectResponse.Metadata.Add(SFS3Client.SFC_DIGEST, MockS3Client.SfcDigest);

                return Task.FromResult(getObjectResponse);
            }
            else
            {
                throw CreateMockAwsResponseError(statusCode, isAsync);
            }
        }

        // Create mock response for UploadFile
        static internal Task<PutObjectResponse> CreateResponseForUploadFile(string statusCode, bool isAsync)
        {
            if (statusCode == HttpStatusCode.OK.ToString())
            {
                return Task.FromResult(new PutObjectResponse());
            }
            else
            {
                throw CreateMockAwsResponseError(statusCode, isAsync);
            }
        }

        // Create mock response for DownloadFile
        static internal Task<GetObjectResponse> CreateResponseForDownloadFile(string statusCode, bool isAsync)
        {
            if (statusCode == HttpStatusCode.OK.ToString())
            {
                var getObjectResponse = new GetObjectResponse();
                byte[] bytes = Encoding.UTF8.GetBytes(MockS3Client.S3FileContent);
                getObjectResponse.ResponseStream = new MemoryStream(bytes);

                return Task.FromResult(getObjectResponse);
            }
            else
            {
                throw CreateMockAwsResponseError(statusCode, isAsync);
            }
        }
    }
}
