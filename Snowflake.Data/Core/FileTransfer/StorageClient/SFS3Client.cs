/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// The S3 client used to transfer files to the remote S3 storage.
    /// </summary>
    class SFS3Client : ISFRemoteStorageClient
    {
        /// <summary>
        /// The metadata of the S3 file.
        /// </summary>
        internal class S3Metadata
        {
            public string HTTP_HEADER_CONTENT_TYPE { get; set; }
            public string SFC_DIGEST { get; set; }
            public string AMZ_IV { get; set; }
            public string AMZ_KEY { get; set; }
            public string AMZ_MATDESC { get; set; }
        }

        /// <summary>
        /// The metadata header keys.
        /// </summary>
        private const string AMZ_META_PREFIX = "x-amz-meta-";
        private const string AMZ_IV = AMZ_META_PREFIX + "x-amz-iv";
        private const string AMZ_KEY = AMZ_META_PREFIX + "x-amz-key";
        private const string AMZ_MATDESC = AMZ_META_PREFIX + "x-amz-matdesc";
        private const string SFC_DIGEST = AMZ_META_PREFIX + "sfc-digest";

        /// <summary>
        /// The application header type.
        /// </summary>
        private const string HTTP_HEADER_VALUE_OCTET_STREAM = "application/octet-stream";

        /// <summary>
        /// The attribute in the credential map containing the aws access key id.
        /// </summary>
        private static readonly string AWS_KEY_ID = "AWS_KEY_ID";

        /// <summary>
        /// The attribute in the credential map containing the aws secret key id.
        /// </summary>
        private static readonly string AWS_SECRET_KEY = "AWS_SECRET_KEY";

        /// <summary>
        /// The attribute in the credential map containing the aws token.
        /// </summary>
        private static readonly string AWS_TOKEN = "AWS_TOKEN";


        /// <summary>
        /// SHA256 hash of an empty request body
        /// </summary>
        public const string EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        /// <summary>
        /// Some common headers
        /// </summary>
        public const string HOST = "Host";
        public const string AUTHORIZATION = "Authorization";
        public const string CONTENT_TYPE = "content-type";
        public const string CONTENT_LENGTH = "content-length";
        public const string X_AMZ_SIGNEDHEADERS = "X-Amz-SignedHeaders";
        public const string X_AMZ_DATE = "X-Amz-Date";
        public const string X_AMZ_CONTENT_SHA256 = "X-Amz-Content-SHA256";
        public const string X_AMZ_SECURITY_TOKEN = "X-Amz-Security-Token";

        /// <summary>
        /// Format strings for the date/time and date stamps required during signing
        /// </summary>
        public const string ISO8601BasicFormat = "yyyyMMddTHHmmssZ";
        public const string DateStringFormat = "yyyyMMdd";

        /// <summary>
        /// The name of the keyed hash algorithm used in signing
        /// </summary>
        public const string ServiceName = "s3";
        public const string Algorithm = "AWS4-HMAC-SHA256";

        /// <summary>
        /// The logger.
        /// </summary>
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFS3Client>();

        /// <summary>
        /// S3 client without client-side encryption.
        /// </summary>
        public SFS3Client()
        {
            Logger.Debug("Setting up a new AWS client ");
        }

        /// <summary>
        /// Extract the bucket name and path from the stage location.
        /// </summary>
        /// <param name="stageLocation">The command stage location.</param>
        /// <returns>The remote location of the S3 file.</returns>
        public RemoteLocation ExtractBucketNameAndPath(string stageLocation)
        {
            // Expand '~' and '~user' expressions
            //if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            //{
            //    stageLocation = Path.GetFullPath(stageLocation);
            //}

            string bucketName = stageLocation;
            string s3path = "";

            // Split stage location as bucket name and path
            if (stageLocation.Contains("/"))
            {
                bucketName = stageLocation.Substring(0, stageLocation.IndexOf('/'));

                s3path = stageLocation.Substring(stageLocation.IndexOf('/') + 1,
                    stageLocation.Length - stageLocation.IndexOf('/') - 1);
                if (s3path != null && !s3path.EndsWith("/"))
                {
                    s3path += '/';
                }
            }

            return new RemoteLocation()
            {
                bucket = bucketName,
                key = s3path
            };
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <returns>The file header of the S3 file.</returns>
        public FileHeader GetFileHeader(SFFileMetadata fileMetadata)
        {
            Uri uri = GetFileUri(fileMetadata);

            // Use empty string hash for content
            SortedDictionary<string, string> headers = new SortedDictionary<string, string>
            {
                {X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256},
                {X_AMZ_SECURITY_TOKEN, fileMetadata.stageInfo.stageCredentials[AWS_TOKEN]},
                {CONTENT_TYPE, HTTP_HEADER_VALUE_OCTET_STREAM}
            };

            string authorization = Sign(
                fileMetadata.stageInfo, 
                EMPTY_BODY_SHA256, 
                "GET", 
                uri, 
                headers);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestHeaders(request, headers, "GET", authorization);

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    //Update the result status of the file metadata
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                    return HandleFileHeaderResponse(response);
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileHeaderErr(response.StatusCode).ToString();                

                return null;
            }
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The file header of the S3 file.</returns>
        public async Task<FileHeader> GetFileHeaderAsync(SFFileMetadata fileMetadata, CancellationToken cancellationToken)
        {
            Uri uri = GetFileUri(fileMetadata);

            // Use empty string hash for content
            SortedDictionary<string, string> headers = new SortedDictionary<string, string>
            {
                {X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256},
                {X_AMZ_SECURITY_TOKEN, fileMetadata.stageInfo.stageCredentials[AWS_TOKEN]},
                {CONTENT_TYPE, HTTP_HEADER_VALUE_OCTET_STREAM}
            };

            string authorization = Sign(
                fileMetadata.stageInfo,
                EMPTY_BODY_SHA256,
                "GET",
                uri,
                headers);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestHeaders(request, headers, "GET", authorization);

                using (HttpWebResponse response = (HttpWebResponse) await request.GetResponseAsync())
                {
                    // Update the result status of the file metadata
                    fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
                    return HandleFileHeaderResponse(response);
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileHeaderErr(response.StatusCode).ToString();

                return null;
            }
        }

        /// <summary>
        /// Get the file header.
        /// </summary>
        /// <param name="response">The Amazon S3 response.</param>
        /// <returns>The file header of the S3 file.</returns>
        private FileHeader HandleFileHeaderResponse(HttpWebResponse response)
        {
            SFEncryptionMetadata encryptionMetadata = new SFEncryptionMetadata
            {
                iv = response.Headers.GetValues(AMZ_IV)[0],
                key = response.Headers.GetValues(AMZ_KEY)[0],
                matDesc = response.Headers.GetValues(AMZ_MATDESC)[0]
            };

            return new FileHeader
            {
                digest = response.Headers.GetValues(SFC_DIGEST)[0],
                contentLength = Convert.ToInt64(response.Headers.GetValues("Content-Length")[0]),
                encryptionMetadata = encryptionMetadata
            };
        }

        /// <summary>
        /// Upload the file to the S3 location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public void UploadFile(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata)
        {
            Uri uri = GetFileUri(fileMetadata);

            // Precompute hash of the body content
            var fileContentHashString = HexEncode(Hash(fileBytes));

            SortedDictionary<string, string> headers = new SortedDictionary<string, string>
            {
                {X_AMZ_CONTENT_SHA256, fileContentHashString},
                {X_AMZ_SECURITY_TOKEN, fileMetadata.stageInfo.stageCredentials[AWS_TOKEN]},
                {CONTENT_TYPE, HTTP_HEADER_VALUE_OCTET_STREAM},
                {CONTENT_LENGTH, fileBytes.Length.ToString()},
                {AMZ_IV, encryptionMetadata.iv},
                {AMZ_KEY, encryptionMetadata.key},
                {AMZ_MATDESC, encryptionMetadata.matDesc},
                {SFC_DIGEST, fileMetadata.sha256Digest},
            };

            string authorization = Sign(
                fileMetadata.stageInfo,
                fileContentHashString,
                "PUT",
                uri,
                headers);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestHeaders(request, headers, "PUT", authorization);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                request.GetResponse();
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileLoadErr(response.StatusCode).ToString();

                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Upload the file to the S3 location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fileBytes">The file bytes to upload.</param>
        /// <param name="encryptionMetadata">The encryption metadata for the header.</param>
        public async Task UploadFileAsync(SFFileMetadata fileMetadata, byte[] fileBytes, SFEncryptionMetadata encryptionMetadata, CancellationToken cancellationToken)
        {
            Uri uri = GetFileUri(fileMetadata);

            // Precompute hash of the body content
            var fileContentHashString = HexEncode(Hash(fileBytes));

            SortedDictionary<string, string> headers = new SortedDictionary<string, string>
            {
                {X_AMZ_CONTENT_SHA256, fileContentHashString},
                {X_AMZ_SECURITY_TOKEN, fileMetadata.stageInfo.stageCredentials[AWS_TOKEN]},
                {CONTENT_TYPE, HTTP_HEADER_VALUE_OCTET_STREAM},
                {CONTENT_LENGTH, fileBytes.Length.ToString()},
                {AMZ_IV, encryptionMetadata.iv},
                {AMZ_KEY, encryptionMetadata.key},
                {AMZ_MATDESC, encryptionMetadata.matDesc},
                {SFC_DIGEST, fileMetadata.sha256Digest},
            };

            string authorization = Sign(
                fileMetadata.stageInfo,
                fileContentHashString,
                "PUT",
                uri,
                headers);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestHeaders(request, headers, "PUT", authorization);

                Stream dataStream = request.GetRequestStream();
                dataStream.Write(fileBytes, 0, fileBytes.Length);
                dataStream.Close();

                await request.GetResponseAsync();
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileLoadErr(response.StatusCode).ToString();

                return;
            }

            fileMetadata.destFileSize = fileMetadata.uploadSize;
            fileMetadata.resultStatus = ResultStatus.UPLOADED.ToString();
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public void DownloadFile(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency)
        {
            Uri uri = GetFileUri(fileMetadata);

            // for a simple GET, we have no body so supply the precomputed 'empty' hash
            SortedDictionary<string, string> headers = new SortedDictionary<string, string>
            {
                {X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256},
                {X_AMZ_SECURITY_TOKEN, fileMetadata.stageInfo.stageCredentials[AWS_TOKEN]},
                {CONTENT_TYPE, HTTP_HEADER_VALUE_OCTET_STREAM}
            };

            string authorization = Sign(
                fileMetadata.stageInfo,
                EMPTY_BODY_SHA256,
                "GET",
                uri,
                headers);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestHeaders(request, headers, "GET", authorization);                

                // Write to file
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (var fileStream = File.Create(fullDstPath))
                using (var responseStream = response.GetResponseStream())
                {
                    responseStream.CopyTo(fileStream);
                    responseStream.Flush();
                }                
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileLoadErr(response.StatusCode).ToString();

                return;
            }

            // Update the result status of the file metadata
            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        /// <summary>
        /// Download the file to the local location.
        /// </summary>
        /// <param name="fileMetadata">The S3 file metadata.</param>
        /// <param name="fullDstPath">The local location to store downloaded file into.</param>
        /// <param name="maxConcurrency">Number of max concurrency.</param>
        public async Task DownloadFileAsync(SFFileMetadata fileMetadata, string fullDstPath, int maxConcurrency, CancellationToken cancellationToken)
        {
            Uri uri = GetFileUri(fileMetadata);

            // for a simple GET, we have no body so supply the precomputed 'empty' hash
            SortedDictionary<string, string> headers = new SortedDictionary<string, string>
            {
                {X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256},
                {X_AMZ_SECURITY_TOKEN, fileMetadata.stageInfo.stageCredentials[AWS_TOKEN]},
                {CONTENT_TYPE, HTTP_HEADER_VALUE_OCTET_STREAM}
            };

            string authorization = Sign(
                fileMetadata.stageInfo,
                EMPTY_BODY_SHA256,
                "GET",
                uri,
                headers);

            try
            {
                // Send the request
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri.AbsoluteUri);

                BuildRequestHeaders(request, headers, "GET", authorization);

                // Write to file
                using (HttpWebResponse response = (HttpWebResponse) await request.GetResponseAsync())
                using (var fileStream = File.Create(fullDstPath))
                using (var responseStream = response.GetResponseStream())
                { 
                    responseStream.CopyTo(fileStream);
                    responseStream.Flush();                    
                }
            }
            catch (WebException ex)
            {
                fileMetadata.lastError = ex;

                HttpWebResponse response = (HttpWebResponse)ex.Response;
                fileMetadata.resultStatus = HandleFileLoadErr(response.StatusCode).ToString();

                return;
            }

            // Update the result status of the file metadata
            fileMetadata.resultStatus = ResultStatus.DOWNLOADED.ToString();
        }

        /// <summary>
        /// Get the result status based on HTTP status code.
        /// </summary>
        /// <param name="statusCode">The HTTP error status code.</param>
        /// <returns>The file's result status.</returns>
        internal ResultStatus HandleFileHeaderErr(HttpStatusCode statusCode)
        {
            if (statusCode == HttpStatusCode.BadRequest)
            {
                return ResultStatus.RENEW_TOKEN;
            }
            else if (statusCode == HttpStatusCode.NotFound)
            {
                return ResultStatus.NOT_FOUND_FILE;
            }
            else
            {
                return ResultStatus.ERROR;
            }
        }

        /// <summary>
        /// Get the result status based on HTTP status code.
        /// </summary>
        /// <param name="statusCode">The HTTP error status code.</param>
        /// <returns>The file's result status.</returns>
        internal ResultStatus HandleFileLoadErr(HttpStatusCode statusCode)
        {
            if (statusCode == HttpStatusCode.BadRequest)
            {
                return ResultStatus.RENEW_TOKEN;
            }
            else
            {
                return ResultStatus.NEED_RETRY;
            }
        }

        /// <summary>
        /// Get the URI from the metadata stage info
        /// </summary>
        /// <param name="fileMetadata"></param>
        /// <returns></returns>
        private Uri GetFileUri(SFFileMetadata fileMetadata)
        {
            PutGetStageInfo stageInfo = fileMetadata.stageInfo;
            RemoteLocation location = ExtractBucketNameAndPath(stageInfo.location);

            string endpointUri = string.Format("https://{0}.s3-{1}.amazonaws.com/{2}",
                                   location.bucket,
                                   stageInfo.region,
                                   location.key + fileMetadata.destFileName);

            return new Uri(endpointUri);
        }

        /// <summary>
        /// Build the headers for the request
        /// </summary>
        /// <param name="request"></param>
        /// <param name="headers"></param>
        /// <param name="method"></param>
        /// <param name="authorization"></param>
        private void BuildRequestHeaders(HttpWebRequest request, SortedDictionary<string, string> headers, string method, string authorization)
        {
            request.Method = method;
            request.Headers.Add(AUTHORIZATION, authorization);

            foreach (KeyValuePair<string, string> header in headers)
            {
                if (header.Key == HOST)
                {
                    request.Host = headers[HOST];
                }
                else if (header.Key == CONTENT_TYPE)
                {
                    request.ContentType = headers[CONTENT_TYPE];
                }
                else if (header.Key == CONTENT_LENGTH)
                {
                    request.ContentLength = Convert.ToInt32(headers[CONTENT_LENGTH]);
                }
                else
                {
                    request.Headers.Add(header.Key, header.Value);
                }
            }
        }

        /// <summary>
        /// Sign the AWS request. Based off https://stackoverflow.com/q/56038177
        /// </summary>
        /// <param name="stageInfo"></param>
        /// <param name="fileContentHashString"></param>
        /// <param name="requestMethod"></param>
        /// <param name="canonicalUri"></param>
        /// <param name="headers"></param>
        /// <returns>The signature for the AWS request</returns>
        private string Sign(PutGetStageInfo stageInfo,
            string fileContentHashString,
            string requestMethod,
            Uri canonicalUri,
            SortedDictionary<string, string> headers)
        {
            // Get current time and convert to ISO8601 format
            var requestDateTime = DateTime.UtcNow;
            var dateTimeStamp = requestDateTime.ToString(ISO8601BasicFormat, CultureInfo.InvariantCulture);

            // Update the headers with required 'x-amz-date' and 'host' values
            headers.Add(X_AMZ_DATE, dateTimeStamp);
            headers.Add("Host", canonicalUri.Host);

            // Convert the headers to a multi-line string in "key:value" format and trim out white spaces
            string canonicalHeaders = string.Join("\n", headers.Select(x => x.Key.ToLowerInvariant() + ":" + x.Value.Trim())) + "\n";

            // Make a string containing all header keys
            string signedHeaders = string.Join(";", headers.Select(x => x.Key.ToLowerInvariant()));

            // Task 1: Create a Canonical Request For Signature Version 4
            string canonicalRequest = requestMethod + "\n" + 
                canonicalUri.AbsolutePath + "\n" + 
                "\n" + // extra newline for query params
                canonicalHeaders + "\n" +
                signedHeaders + "\n" +
                fileContentHashString;

            string hashedCanonicalRequest = HexEncode(Hash(ToBytes(canonicalRequest)));

            // Task 2: Create a String to Sign for Signature Version 4
            var dateStamp = requestDateTime.ToString(DateStringFormat, CultureInfo.InvariantCulture);
            var credentialScope = string.Format("{0}/{1}/{2}/aws4_request", dateStamp, stageInfo.region, ServiceName);

            string stringToSign = Algorithm + "\n" + dateTimeStamp + "\n" + credentialScope + "\n" + hashedCanonicalRequest;

            // Task 3: Calculate the signature for AWS Signature Version 4
            byte[] signingKey = GetSignatureKey(stageInfo.stageCredentials[AWS_SECRET_KEY], dateStamp, stageInfo.region, ServiceName);
            string signature = HexEncode(HmacSha256(stringToSign, signingKey));

            // Task 4: Add the signature to the HTTP request
            // Authorization: algorithm Credential=access key ID/credential scope, SignedHeaders=SignedHeaders, Signature=signature
            string authorization = string.Format("{0} Credential={1}/{2}/{3}/{4}/aws4_request, SignedHeaders={5}, Signature={6}",
            Algorithm, stageInfo.stageCredentials[AWS_KEY_ID], dateStamp, stageInfo.region, ServiceName, signedHeaders, signature);

            return authorization;
        }
        /// <summary>
        /// Get signature key using AWS algorithm
        /// Based off https://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-dotnet
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dateStamp"></param>
        /// <param name="regionName"></param>
        /// <param name="serviceName"></param>
        /// <returns>The hash using HMACSHA256 algorithm</returns>
        private byte[] GetSignatureKey(string key, string dateStamp, string regionName, string serviceName)
        {
            byte[] kDate = HmacSha256(dateStamp, ToBytes("AWS4" + key));
            byte[] kRegion = HmacSha256(regionName, kDate);
            byte[] kService = HmacSha256(serviceName, kRegion);
            return HmacSha256("aws4_request", kService);
        }

        private byte[] ToBytes(string str)
        {
            return Encoding.UTF8.GetBytes(str.ToCharArray());
        }

        private string HexEncode(byte[] bytes)
        {
            return BitConverter.ToString(bytes).Replace("-", string.Empty).ToLowerInvariant();
        }
        private byte[] Hash(byte[] bytes)
        {
            return SHA256.Create().ComputeHash(bytes);
        }
        private byte[] HmacSha256(string data, byte[] key)
        {
            return new HMACSHA256(key).ComputeHash(ToBytes(data));
        }
    }
}