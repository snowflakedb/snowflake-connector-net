/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Amazon.S3;
using Amazon.S3.Model;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.IO;
using System.Text;
using System.Net.Http;
using System.Net;

namespace Snowflake.Data.Tests.Mock
{
    class MockAmazonS3Client : AmazonS3Client
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

        public MockAmazonS3Client(string awsAccessKeyId, string awsSecretAccessKey, string awsSessionToken, AmazonS3Config clientConfig)
    : base(awsAccessKeyId, awsSecretAccessKey, awsSessionToken, clientConfig)
        {
        }

        internal Exception CreateMockAwsResponseError(string errorCode, string isAsync)
        {
            AmazonS3Exception awsError = new AmazonS3Exception(S3ErrorMessage);
            awsError.ErrorCode = errorCode;

            if (!String.IsNullOrEmpty(isAsync))
            {
                return awsError; // S3 throws the AmazonS3Exception on async calls
            }

            Exception exceptionContainingS3Error = new Exception(S3ErrorMessage, awsError);
            return exceptionContainingS3Error;  // S3 places the AmazonS3Exception on the InnerException property on non-async calls
        }

        public override Task<GetObjectResponse> GetObjectAsync(GetObjectRequest request, CancellationToken cancellationToken = default(CancellationToken))
        {
            string key = request.BucketName;

            string[] requestKey = request.Key.Split('/');
            string method = requestKey[0];
            string isAsync = requestKey[1];

            if (key == HttpStatusCode.OK.ToString())
            {
                GetObjectResponse getObjectResponse = new GetObjectResponse();

                if (method == HttpMethod.Head.ToString())
                {
                    getObjectResponse.ContentLength = ContentLength;
                    getObjectResponse.Metadata.Add(SFS3Client.AMZ_IV, AmzIV);
                    getObjectResponse.Metadata.Add(SFS3Client.AMZ_KEY, AmzKey);
                    getObjectResponse.Metadata.Add(SFS3Client.AMZ_MATDESC, AmzMatdesc);
                    getObjectResponse.Metadata.Add(SFS3Client.SFC_DIGEST, SfcDigest);
                }
                else if (method == HttpMethod.Get.ToString())
                {
                    byte[] bytes = Encoding.UTF8.GetBytes(S3FileContent);
                    getObjectResponse.ResponseStream = new MemoryStream(bytes);
                }

                return Task.FromResult(getObjectResponse);
            }
            else if (key == SFS3Client.EXPIRED_TOKEN)
            {
                throw CreateMockAwsResponseError(SFS3Client.EXPIRED_TOKEN, isAsync);
            }
            else if (key == SFS3Client.NO_SUCH_KEY && method == HttpMethod.Head.ToString())
            {
                throw CreateMockAwsResponseError(SFS3Client.NO_SUCH_KEY, isAsync);
            }
            else
            {
                throw CreateMockAwsResponseError(AwsStatusError, isAsync);
            }
        }

        public override Task<PutObjectResponse> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default(CancellationToken))
        {
            string key = request.BucketName;

            string[] requestKey = request.Key.Split('/');
            string method = requestKey[0];
            string isAsync = requestKey[1];

            if (key == HttpStatusCode.OK.ToString() && method == HttpMethod.Put.ToString())
            {
                return Task.FromResult(new PutObjectResponse());
            }
            else if (key == SFS3Client.EXPIRED_TOKEN)
            {
                throw CreateMockAwsResponseError(SFS3Client.EXPIRED_TOKEN, isAsync);
            }
            else
            {
                throw CreateMockAwsResponseError(AwsStatusError, isAsync);
            }
        }
    }
}
