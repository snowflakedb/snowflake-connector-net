/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using Moq;
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

        // Mock data for downloaded file
        public const string GcsFileContent = "GCSClientTest";

        public MockGCSWebRequest(string requestUriString)
        {
            requestUri = new Uri(requestUriString);
            Headers = new WebHeaderCollection();
        }

        protected WebResponse CreateMockWebResponse(string key, string method)
        {
            var response = new Mock<HttpWebResponse>();

            if (key == HttpStatusCode.OK.ToString())
            {
                response.Setup(c => c.Headers).Returns(new WebHeaderCollection());

                if (method == HttpMethod.Head.ToString())
                {
                    response.Object.Headers.Add("content-length", FileContentLength.ToString());
                    response.Object.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                }
                else if (method == HttpMethod.Put.ToString())
                {
                    // Upload file
                }
                else if (method == HttpMethod.Get.ToString())
                {
                    response.Setup(c => c.GetResponseStream()).Returns(new MemoryStream(Encoding.ASCII.GetBytes(GcsFileContent)));

                    response.Object.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                        "{" +
                        $"\"ContentEncryptionIV\": \"{GcsIV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{GcsKey}\"}}" +
                        "}");
                    response.Object.Headers.Add(SFGCSClient.GCS_METADATA_MATDESC_KEY, GcsMatdesc);
                    response.Object.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                    response.Object.Headers.Add(SFGCSClient.GCS_FILE_HEADER_CONTENT_LENGTH, FileContentLength.ToString());
                }
            }
            else
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns((HttpStatusCode)Enum.Parse(typeof(HttpStatusCode), key));
                throw new WebException("Mock GCS Error", null, 0, response.Object);
            }

            return response.Object;
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
#pragma warning restore SYSLIB0014 // Type or member is obsolete
}
