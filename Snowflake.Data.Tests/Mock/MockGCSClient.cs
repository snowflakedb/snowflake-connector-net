/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{
    class MockGCSHttpClient : HttpClientHandler
    {
        // Mock GCS data for FileHeader
        public const string GcsIV = "MOCK_GCS";
        public const string GcsKey = "MOCK_GCS_KEY";
        public const string GcsMatdesc = "MOCK_GCS_MATDESC";
        public const string SFCDigest = "MOCK_SFC_DIGEST";

        // Mock data for downloaded file
        public const string GcsFileContent = "GCSClientTest";

        // Mock content length
        public const int ContentLength = 9999;

        internal void SetHttpResponseStatusCode(HttpResponseMessage response, string key)
        {
            HttpStatusCode statusCode = (HttpStatusCode)Enum.Parse(typeof(HttpStatusCode), key);

            switch (statusCode)
            {
                case HttpStatusCode.BadRequest:
                    response.StatusCode = HttpStatusCode.BadRequest;
                    break;
                case HttpStatusCode.NotFound:
                    response.StatusCode = HttpStatusCode.NotFound;
                    break;
                case HttpStatusCode.Unauthorized:
                    response.StatusCode = HttpStatusCode.Unauthorized;
                    break;
                case HttpStatusCode.Forbidden:
                    response.StatusCode = HttpStatusCode.Forbidden;
                    break;
                case HttpStatusCode.InternalServerError:
                    response.StatusCode = HttpStatusCode.InternalServerError;
                    break;
                case HttpStatusCode.ServiceUnavailable:
                    response.StatusCode = HttpStatusCode.ServiceUnavailable;
                    break;
                default:
                    response.StatusCode = HttpStatusCode.Ambiguous;
                    break;
            }
        }

        protected HttpResponseMessage CreateMockHttpResponse(string key, HttpMethod method)
        {
            HttpResponseMessage response = new HttpResponseMessage();

            if (key == HttpStatusCode.OK.ToString())
            {
                if (method == HttpMethod.Head)
                {
                    response.Content = new StringContent("");
                    response.Content.Headers.ContentLength = ContentLength;
                    response.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                }
                else if (method == HttpMethod.Put)
                {
                    // Upload file
                }
                else if (method == HttpMethod.Get)
                {
                    response.Content = new StringContent(GcsFileContent);
                    response.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                        "{" +
                        $"\"ContentEncryptionIV\": \"{GcsIV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{GcsKey}\"}}" +
                        "}");
                    response.Headers.Add(SFGCSClient.GCS_METADATA_MATDESC_KEY, GcsMatdesc);
                    response.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                    response.Headers.Add(SFGCSClient.GCS_FILE_HEADER_CONTENT_LENGTH, ContentLength.ToString());
                }
            }
            else
            {
                SetHttpResponseStatusCode(response, key);
            }

            return response;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string[] requestKey = request.RequestUri.AbsolutePath.ToString().Split('/');
            string key = requestKey[1]; // Skip the first index

            HttpResponseMessage response = CreateMockHttpResponse(key, request.Method);

            return Task.FromResult(response);
        }
    }
}
