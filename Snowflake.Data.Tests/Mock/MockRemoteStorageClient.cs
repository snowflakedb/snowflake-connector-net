using Moq;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using System;
using System.IO;
using System.Net;
using System.Text;

namespace Snowflake.Data.Tests.Mock
{

    class MockRemoteStorageClient
    {
        // Mock data for downloaded file
        internal const string FileContent = "RemoteStorageClientTest";

        // Mock content length
        internal const int ContentLength = 9999;

        // Mock error message
        internal const string ErrorMessage = "Mock GCS Remote Storage Error";

        // Variables for the encryption data
        [ThreadStatic] static Stream t_encryptedStream = null;
        [ThreadStatic] static string t_encryptedStreamIV = null;
        [ThreadStatic] static string t_encryptedStreamKey = null;

        static internal void SetEncryptionData(Stream stream, string iv, string key)
        {
            t_encryptedStream = stream;
            t_encryptedStreamIV = iv;
            t_encryptedStreamKey = key;
        }

        // Sets up the mock sequence when Remote Storage uploads a file
        // 1. Get the file header
        // 2. Upload the file
        static internal HttpWebResponse SequenceResponseForUploadFile(ref bool firstRequest, HttpStatusCode statusCode, HttpStatusCode statusCodeAfterRetry)
        {
            try
            {
                // For the first call to GetResponse(), return the file header response
                // For the second call to GetResponse(), return the upload response
                var response = firstRequest ?
                        CreateResponseForFileHeader(statusCode) :
                        CreateResponseForUploadFile(statusCodeAfterRetry);

                return response;
            }
            finally
            {
                // If an exception is thrown, the value will still be changed
                firstRequest = false;
            }
        }

        // Create a mock response for GetFileHeader
        static internal HttpWebResponse CreateResponseForFileHeader(HttpStatusCode httpStatusCode)
        {
            var response = new Mock<HttpWebResponse>();

            if (httpStatusCode == HttpStatusCode.OK)
            {
                response.Setup(c => c.Headers).Returns(new WebHeaderCollection());
                response.Object.Headers.Add("content-length", MockGCSClient.ContentLength.ToString());
                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, MockGCSClient.SFCDigest);
            }
            else
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns(httpStatusCode);
                throw new WebException(ErrorMessage, null, 0, response.Object);
            }

            return response.Object;
        }

        // Create a mock response for UploadFile
        static internal HttpWebResponse CreateResponseForUploadFile(HttpStatusCode httpStatusCode)
        {
            var response = new Mock<HttpWebResponse>();

            if (httpStatusCode != HttpStatusCode.OK)
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns(httpStatusCode);
                throw new WebException(ErrorMessage, null, 0, response.Object);
            }

            return response.Object;
        }

        // Create a mock response for DownloadFile
        static internal HttpWebResponse CreateResponseForDownloadFile(HttpStatusCode httpStatusCode)
        {
            var response = new Mock<HttpWebResponse>();

            if (httpStatusCode == HttpStatusCode.OK)
            {
                response.Setup(c => c.Headers).Returns(new WebHeaderCollection());

                // For downloads with encryption material
                if (t_encryptedStream != null)
                {
                    // Set the position to 0 and return the encrypted stream
                    t_encryptedStream.Position = 0;
                    response.Setup(c => c.GetResponseStream()).Returns(t_encryptedStream);

                    // Set the iv and key to the ones used for encrypting the stream
                    response.Object.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                        "{" +
                        $"\"ContentEncryptionIV\": \"{t_encryptedStreamIV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{t_encryptedStreamKey}\"}}" +
                        "}");

                    t_encryptedStreamIV = null;
                    t_encryptedStreamKey = null;
                    t_encryptedStream = null;
                }
                else // For unencrypted downloads
                {
                    response.Setup(c => c.GetResponseStream()).Returns(new MemoryStream(Encoding.ASCII.GetBytes(FileContent)));
                    response.Object.Headers.Add(SFGCSClient.GCS_METADATA_ENCRYPTIONDATAPROP,
                        "{" +
                        $"\"ContentEncryptionIV\": \"{MockGCSClient.GcsIV}\", " +
                        $"\"WrappedContentKey\": {{\"EncryptedKey\":\"{MockGCSClient.GcsKey}\"}}" +
                        "}");
                }

                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_MATDESC_KEY, MockGCSClient.GcsMatdesc);
                response.Object.Headers.Add(SFGCSClient.GCS_METADATA_SFC_DIGEST, MockGCSClient.SFCDigest);
                response.Object.Headers.Add(SFGCSClient.GCS_FILE_HEADER_CONTENT_LENGTH, MockGCSClient.ContentLength.ToString());
            }
            else
            {
                response.SetupGet(c => c.StatusCode)
                    .Returns(httpStatusCode);
                throw new WebException(ErrorMessage, null, 0, response.Object);
            }

            return response.Object;
        }
    }
}

