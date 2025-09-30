using Moq;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using System.IO;
using System.Net;
using System.Text;

namespace Snowflake.Data.Tests.Mock
{
    class MockGCSClient
    {
        // Mock GCS data for FileHeader
        internal const string GcsIV = "MOCK_GCS";
        internal const string GcsKey = "MOCK_GCS_KEY";
        internal const string GcsMatdesc = "MOCK_GCS_MATDESC";
        internal const string SFCDigest = "MOCK_SFC_DIGEST";

        // Mock content length
        internal const int ContentLength = 9999;

        // Mock data for downloaded file
        internal const string GcsFileContent = "GCSClientTest";

        // Create a mock response for GetFileHeader
        internal static HttpWebResponse CreateResponseForFileHeader(HttpStatusCode httpStatusCode)
        {
            var response = new Mock<HttpWebResponse>();

            if (httpStatusCode == HttpStatusCode.OK)
            {
                response.Setup(c => c.Headers).Returns(new WebHeaderCollection());
                response.Object.Headers.Add("content-length", ContentLength.ToString());
                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
            }
            else
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns(httpStatusCode);
                throw new WebException("Mock GCS Error", null, 0, response.Object);
            }

            return response.Object;
        }

        // Create a mock response for UploadFile
        internal static HttpWebResponse CreateResponseForUploadFile(HttpStatusCode? httpStatusCode)
        {
            var response = new Mock<HttpWebResponse>();

            if (httpStatusCode is null)
            {
                throw new WebException("Mock GCS Error - no response", null, 0, null);
            }
            else if (httpStatusCode != HttpStatusCode.OK)
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns(httpStatusCode.Value);
                throw new WebException("Mock GCS Error", null, 0, response.Object);
            }

            return response.Object;
        }

        // Create a mock response for DownloadFile
        internal static HttpWebResponse CreateResponseForDownloadFile(HttpStatusCode? httpStatusCode)
        {
            var response = new Mock<HttpWebResponse>();

            if (httpStatusCode is null)
            {
                throw new WebException("Mock GCS Error - no response", null, 0, null);
            }
            else if (httpStatusCode == HttpStatusCode.OK)
            {
                response.Setup(c => c.Headers).Returns(new WebHeaderCollection());
                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                    "{" +
                    $"\"ContentEncryptionIV\": \"{GcsIV}\", " +
                    $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{GcsKey}\"}}" +
                    "}");
                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_MATDESC_KEY, GcsMatdesc);
                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, SFCDigest);
                response.Object.Headers.Add(SFGCSClient.GCS_FILE_HEADER_CONTENT_LENGTH, ContentLength.ToString());

                response.Setup(c => c.GetResponseStream()).Returns(new MemoryStream(Encoding.ASCII.GetBytes(GcsFileContent)));
            }
            else
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns(httpStatusCode.Value);
                throw new WebException("Mock GCS Error", null, 0, response.Object);
            }

            return response.Object;
        }
    }
}
