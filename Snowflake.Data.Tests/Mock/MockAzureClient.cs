/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Metadata = System.Collections.Generic.IDictionary<string, string>;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System;

namespace Snowflake.Data.Tests.Mock
{
    class MockBlobClient : BlobClient
    {
        // Mock Azure data for FileHeader
        internal const string AzureIV = "MOCK_AZURE_IV";
        internal const string AzureKey = "MOCK_AZURE_KEY";
        internal const string AzureMatdesc = "MOCK_AZURE_MATDESC";
        internal const string SFCDigest = "MOCK_SFC_DIGEST";

        // Mock error message for Azure errors
        internal const string AzureErrorMessage = "Azure Error Message";

        // Mock content length
        internal const int ContentLength = 9999;

        // Stores the HttpStatusCode string
        string _key;

        internal MockBlobClient(string blobName) { _key = MockBlobContainerClient.blobContainerName; }

        internal Exception CreateMockAzureError(string key)
        {
            HttpStatusCode statusCode = (HttpStatusCode)Enum.Parse(typeof(HttpStatusCode), key);

            RequestFailedException azureError;

            switch (statusCode)
            {
                case HttpStatusCode.BadRequest:
                    azureError = new RequestFailedException((int)HttpStatusCode.BadRequest, AzureErrorMessage);
                    break;
                case HttpStatusCode.NotFound:
                    azureError = new RequestFailedException((int)HttpStatusCode.NotFound, AzureErrorMessage);
                    break;
                case HttpStatusCode.Unauthorized:
                    azureError = new RequestFailedException((int)HttpStatusCode.Unauthorized, AzureErrorMessage);
                    break;
                case HttpStatusCode.Forbidden:
                    azureError = new RequestFailedException((int)HttpStatusCode.Forbidden, AzureErrorMessage);
                    break;
                case HttpStatusCode.InternalServerError:
                    azureError = new RequestFailedException((int)HttpStatusCode.InternalServerError, AzureErrorMessage);
                    break;
                case HttpStatusCode.ServiceUnavailable:
                    azureError = new RequestFailedException((int)HttpStatusCode.ServiceUnavailable, AzureErrorMessage);
                    break;
                default:
                    azureError = new RequestFailedException(0, AzureErrorMessage);
                    break;
            }

            return azureError;
        }

        internal Response<BlobProperties> createMockResponseForBlobProperties()
        {
            if (_key == HttpStatusCode.OK.ToString())
            {
                Dictionary<string, string> metadata = new Dictionary<string, string>
                {
                    { "encryptiondata",
                        "{" +
                        $"\"ContentEncryptionIV\": \"{AzureIV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{AzureKey}\"}}" +
                        "}"
                    },
                    {
                        "matdesc", AzureMatdesc
                    },
                    {
                        "sfcdigest", SFCDigest
                    }
                };

                BlobProperties blobProperties = BlobsModelFactory.BlobProperties(metadata: metadata, contentLength: ContentLength);

                Response<BlobProperties> mockResponse = Response.FromValue(
                            blobProperties,
                            null);

                return mockResponse;
            }
            else
            {
                throw CreateMockAzureError(_key);
            }
        }

        public override Response<BlobProperties> GetProperties(
            BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            return createMockResponseForBlobProperties();
        }

        public override async Task<Response<BlobProperties>> GetPropertiesAsync(
            BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            return await Task.Run(() => createMockResponseForBlobProperties()).ConfigureAwait(false);
        }

        public Response<BlobContentInfo> createMockResponseForBlobContentInfo()
        {
            if (_key == HttpStatusCode.OK.ToString())
            {
                return null;
            }
            else
            {
                throw CreateMockAzureError(_key);
            }
        }

        public override Response<BlobContentInfo> Upload(Stream content)
        {
            return createMockResponseForBlobContentInfo();
        }
        
        public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content,
            CancellationToken cancellationToken = default)
        {
            return await Task.Run(() => createMockResponseForBlobContentInfo()).ConfigureAwait(false);
        }

        public override Response<BlobInfo> SetMetadata(
            Metadata metadata,
            BlobRequestConditions conditions = default,
            CancellationToken cancellationToken = default)
        {
            return null;
        }

        public override Response DownloadTo(string path)
        {
            if (_key == HttpStatusCode.OK.ToString())
            {
                return null;
            }
            else
            {
                throw CreateMockAzureError(_key);
            }
        }

        public override async Task<Response> DownloadToAsync(string path, CancellationToken cancellationToken)
        {
            if (_key == HttpStatusCode.OK.ToString())
            {
                return await Task.Run(() => Task.FromResult<Response>(null)).ConfigureAwait(false);
            }
            else
            {
                throw CreateMockAzureError(_key);
            }
        }
    }

    class MockBlobContainerClient : BlobContainerClient
    {
        internal static string blobContainerName;

        public MockBlobContainerClient(string blobContainerName) { MockBlobContainerClient.blobContainerName = blobContainerName; }

        public override BlobClient GetBlobClient(string blobName)
        {
            return new MockBlobClient(blobName);
        }
    }

    class MockAzureBlobClient : BlobServiceClient
    {
        public override BlobContainerClient GetBlobContainerClient(string blobContainerName) =>
            new MockBlobContainerClient(blobContainerName);
    }
}
