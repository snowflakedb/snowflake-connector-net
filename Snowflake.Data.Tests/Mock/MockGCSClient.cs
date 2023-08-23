/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{
    class MockGCSHttpClient : DelegatingHandler
    {
        // Mock GCS data for FileHeader
        public const string GCS_IV = "MOCK_GCS";
        public const string GCS_KEY = "MOCK_GCS_KEY";
        public const string GCS_MATDESC = "MOCK_GCS_MATDESC";
        public const string SFC_DIGEST = "MOCK_SFC_DIGEST";

        // Mock data for downloaded file
        public const string FILE_CONTENT = "GCSClientTest";

        // Mock content length
        public const int CONTENT_LENGTH = 9999;

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
                    response.Content.Headers.ContentLength = CONTENT_LENGTH;
                    response.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFC_DIGEST);
                }
                else if (method == HttpMethod.Put)
                {
                    // Upload file
                }
                else if (method == HttpMethod.Get)
                {
                    response.Content = new StringContent(FILE_CONTENT);
                    response.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                        "{" +
                        $"\"ContentEncryptionIV\": \"{GCS_IV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{GCS_KEY}\"}}" +
                        "}");
                    response.Headers.Add(SFGCSClient.GCS_METADATA_MATDESC_KEY, GCS_MATDESC);
                    response.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFC_DIGEST);
                    response.Headers.Add(SFGCSClient.GCS_FILE_HEADER_CONTENT_LENGTH, CONTENT_LENGTH.ToString());
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
