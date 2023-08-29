/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Amazon.S3;
using System;

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
        static internal Exception CreateMockAwsResponseError(string errorCode, bool isAsync)
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
    }
}
