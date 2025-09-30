using Azure;
using Azure.Storage.Blobs.Models;
using System.Collections.Generic;
using System.Net;
using System;

namespace Snowflake.Data.Tests.Mock
{
    class MockAzureClient
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

        // Creates the Azure exception for mock requests
        static internal Exception CreateMockAzureError(string key)
        {
            HttpStatusCode statusCode = (HttpStatusCode)Enum.Parse(typeof(HttpStatusCode), key);
            RequestFailedException azureError = new RequestFailedException((int)statusCode, AzureErrorMessage);

            return azureError;
        }

        // Creates the Azure response for blob properties requests
        static internal Response<BlobProperties> createMockResponseForBlobProperties(string key)
        {
            if (key == HttpStatusCode.OK.ToString())
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
                throw CreateMockAzureError(key);
            }
        }

        // Create the Azure response based for bloc content info requests
        static internal Response<BlobContentInfo> createMockResponseForBlobContentInfo(string key)
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
    }
}
