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
        public const string AZURE_IV = "MOCK_AZURE_IV";
        public const string AZURE_KEY = "MOCK_AZURE_KEY";
        public const string AZURE_MATDESC = "MOCK_AZURE_MATDESC";
        public const string SFC_DIGEST = "MOCK_SFC_DIGEST";

        // Mock error message for Azure errors
        public const string AZURE_ERROR_MESSAGE = "Azure Error Message";

        // Mock content length
        public const int CONTENT_LENGTH = 9999;

        // Stores the HttpStatusCode string
        string key;

        public MockBlobClient(string blobName) { key = MockBlobContainerClient.blobContainerName; }

        public Exception CreateMockAzureError(string key)
        {
            HttpStatusCode statusCode = (HttpStatusCode)Enum.Parse(typeof(HttpStatusCode), key);

            RequestFailedException azureError;

            switch (statusCode)
            {
                case HttpStatusCode.BadRequest:
                    azureError = new RequestFailedException((int)HttpStatusCode.BadRequest, AZURE_ERROR_MESSAGE);
                    break;
                case HttpStatusCode.NotFound:
                    azureError = new RequestFailedException((int)HttpStatusCode.NotFound, AZURE_ERROR_MESSAGE);
                    break;
                case HttpStatusCode.Unauthorized:
                    azureError = new RequestFailedException((int)HttpStatusCode.Unauthorized, AZURE_ERROR_MESSAGE);
                    break;
                case HttpStatusCode.Forbidden:
                    azureError = new RequestFailedException((int)HttpStatusCode.Forbidden, AZURE_ERROR_MESSAGE);
                    break;
                case HttpStatusCode.InternalServerError:
                    azureError = new RequestFailedException((int)HttpStatusCode.InternalServerError, AZURE_ERROR_MESSAGE);
                    break;
                case HttpStatusCode.ServiceUnavailable:
                    azureError = new RequestFailedException((int)HttpStatusCode.ServiceUnavailable, AZURE_ERROR_MESSAGE);
                    break;
                default:
                    azureError = new RequestFailedException(0, AZURE_ERROR_MESSAGE);
                    break;
            }

            return azureError;
        }

        public Response<BlobProperties> createMockResponseForBlobProperties()
        {
            if (key == HttpStatusCode.OK.ToString())
            {
                Dictionary<string, string> metadata = new Dictionary<string, string>
                {
                    { "encryptiondata",
                        "{" +
                        $"\"ContentEncryptionIV\": \"{AZURE_IV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{AZURE_KEY}\"}}" +
                        "}"
                    },
                    {
                        "matdesc", AZURE_MATDESC
                    },
                    {
                        "sfcdigest", SFC_DIGEST
                    }
                };

                BlobProperties blobProperties = BlobsModelFactory.BlobProperties(metadata: metadata, contentLength: CONTENT_LENGTH);

                Response<BlobProperties> mockResponse = Response.FromValue(
                            blobProperties,
                            null);

                return mockResponse;
            }
            else
            {
                throw CreateMockAzureError(key);
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
            if (key == HttpStatusCode.OK.ToString())
            {
                return null;
            }
            else
            {
                throw CreateMockAzureError(key);
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
            if (key == HttpStatusCode.OK.ToString())
            {
                return null;
            }
            else
            {
                throw CreateMockAzureError(key);
            }
        }

        public override async Task<Response> DownloadToAsync(string path, CancellationToken cancellationToken)
        {
            if (key == HttpStatusCode.OK.ToString())
            {
                return await Task.Run(() => Task.FromResult<Response>(null)).ConfigureAwait(false);
            }
            else
            {
                throw CreateMockAzureError(key);
            }
        }
    }

    class MockBlobContainerClient : BlobContainerClient
    {
        public static string blobContainerName;

        public MockBlobContainerClient(string blobContainerName) { MockBlobContainerClient.blobContainerName = blobContainerName; }

        public override BlobClient GetBlobClient(string blobName)
        {
            return new MockBlobClient(blobName);
        }
    }

    class MockAzureClient : BlobServiceClient
    {
        public override BlobContainerClient GetBlobContainerClient(string blobContainerName) =>
            new MockBlobContainerClient(blobContainerName);
    }
}
