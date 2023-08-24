/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Mock
{
#pragma warning disable SYSLIB0014 // Type or member is obsolete
    class MockGCSWebRequest : WebRequest
    {
        public Uri requestUri;
        public override WebHeaderCollection Headers { get; set; }

        public override string Method { get; set; }

        public override int Timeout { get; set; }

        public Stream memoryStream = new MemoryStream();

        // Mock GCS data for FileHeader
        public const string GcsIV = "MOCK_GCS";
        public const string GcsKey = "MOCK_GCS_KEY";
        public const string GcsMatdesc = "MOCK_GCS_MATDESC";
        public const string SFCDigest = "MOCK_SFC_DIGEST";

        // Mock content length
        public const int FileContentLength = 9999;

        public MockGCSWebRequest(string requestUriString)
        {
            requestUri = new Uri(requestUriString);
            Headers = new WebHeaderCollection();
        }

        internal WebException SetHttpResponseStatusCode(MockGCSWebResponse response, string key)
        {
            HttpStatusCode statusCode = (HttpStatusCode)Enum.Parse(typeof(HttpStatusCode), key);

            switch (statusCode)
            {
                case HttpStatusCode.BadRequest:
                    Timeout = (int)HttpStatusCode.BadRequest;
                    break;
                case HttpStatusCode.NotFound:
                    Timeout = (int)HttpStatusCode.NotFound;
                    break;
                case HttpStatusCode.Unauthorized:
                    Timeout = (int)HttpStatusCode.Unauthorized;
                    break;
                case HttpStatusCode.Forbidden:
                    Timeout = (int)HttpStatusCode.Forbidden;
                    break;
                case HttpStatusCode.InternalServerError:
                    Timeout = (int)HttpStatusCode.InternalServerError;
                    break;
                case HttpStatusCode.ServiceUnavailable:
                    Timeout = (int)HttpStatusCode.ServiceUnavailable;
                    break;
                default:
                    Timeout = (int)HttpStatusCode.Ambiguous;
                    break;
            }

            return new WebException(null, null, 0, response);
        }

        protected WebResponse CreateMockWebResponse(string key, string method)
        {
            WebResponse response = new MockGCSWebResponse();

            if (key == HttpStatusCode.OK.ToString())
            {
                if (method == HttpMethod.Head.ToString())
                {
                    response.Headers.Add("content-length", FileContentLength.ToString());
                    response.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                }
                else if (method == HttpMethod.Put.ToString())
                {
                    // Upload file
                }
                else if (method == HttpMethod.Get.ToString())
                {
                    response.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                        "{" +
                        $"\"ContentEncryptionIV\": \"{GcsIV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{GcsKey}\"}}" +
                        "}");
                    response.Headers.Add(SFGCSClient.GCS_METADATA_MATDESC_KEY, GcsMatdesc);
                    response.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                    response.Headers.Add(SFGCSClient.GCS_FILE_HEADER_CONTENT_LENGTH, FileContentLength.ToString());
                }
            }
            else
            {
                SetHttpResponseStatusCode((MockGCSWebResponse)response, key);
                throw new WebException("Mock Error");
            }

            return response;
        }

        public override WebResponse GetResponse()
        {
            string[] requestKey = requestUri.AbsolutePath.ToString().Split('/');
            string key = requestKey[1]; // Skip the first index

            WebResponse response = CreateMockWebResponse(key, Method);

            return response;
        }

        public override Task<WebResponse> GetResponseAsync()
        {
            string[] requestKey = requestUri.AbsolutePath.ToString().Split('/');
            string key = requestKey[1]; // Skip the first index

            WebResponse response = CreateMockWebResponse(key, Method);

            return Task.FromResult(response);
        }

        public override Stream GetRequestStream()
        {
            return memoryStream;
        }

        public override Task<Stream> GetRequestStreamAsync()
        {
            return Task.FromResult(memoryStream);
        }
    }

    class MockGCSWebResponse : WebResponse
    {
        // Mock data for downloaded file
        public const string GcsFileContent = "GCSClientTest";
        public override WebHeaderCollection Headers { get; }

        public MockGCSWebResponse()
        {
            Headers = new WebHeaderCollection();
        }

        public override Stream GetResponseStream()
        {
            return new MemoryStream(Encoding.ASCII.GetBytes(GcsFileContent));
        }
    }

#pragma warning restore SYSLIB0014 // Type or member is obsolete
}
