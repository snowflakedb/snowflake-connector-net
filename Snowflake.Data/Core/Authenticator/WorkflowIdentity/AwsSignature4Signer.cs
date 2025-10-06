using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    // following code was inspired by https://github.com/aws-samples/sigv4-signing-examples/blob/main/no-sdk/dotnet/AWSSigner.cs
    internal class AwsSignature4Signer
    {
        private const string V4SignatureAlgorithm = "AWS4-HMAC-SHA256";
        private const string AmazonDateHeader = "x-amz-date";
        private const string AmazonTokenHeader = "x-amz-security-token";
        private const string AuthorizationHeader = "authorization";

        public static void AddTokenAndSignatureHeaders(
            AttestationRequest request,
            AwsConfiguration awsConfiguration,
            DateTime utcNow)
        {
            var httpMethod = request.Method;
            var uri = request.Uri;
            var headers = request.Headers;
            var awsDate = ToAwsDate(utcNow);
            headers.Add(AmazonDateHeader, awsDate);
            headers.Add(AmazonTokenHeader, awsConfiguration.Credentials.Token);
            var dateString = utcNow.ToString("yyyyMMdd");
            var canonicalHeaders = CanonicalizeHeaders(headers);
            var signedHeaders = CanonicalizeHeaderNames(headers);
            var queryParameters = uri.Query.StartsWith("?") ? uri.Query.Substring(1) : uri.Query;
            var canonicalRequest = $"{httpMethod}\n{uri.AbsolutePath}\n{queryParameters}\n{canonicalHeaders}\n{signedHeaders}\n{CalculateHash("")}";
            var credentialScope = $"{dateString}/{awsConfiguration.Region}/{awsConfiguration.Service}/aws4_request";
            var stringToSign = $"{V4SignatureAlgorithm}\n{awsDate}\n{credentialScope}\n{CalculateHash(canonicalRequest)}";
            var signatureKey = GetSignatureKey(dateString, awsConfiguration);
            var signature = CalculateHmacHex(signatureKey, stringToSign);
            var authorization = $"{V4SignatureAlgorithm} Credential={awsConfiguration.Credentials.AccessKey}/{credentialScope}, SignedHeaders={signedHeaders}, Signature={signature}";
            headers.Add(AuthorizationHeader, authorization);
        }

        private static string ToAwsDate(DateTime date) =>
            date.ToString("yyyyMMddTHHmmssZ");

        private static string CalculateHash(string value) =>
            value.ToSha256Hash().ToLowerInvariant();

        static byte[] GetSignatureKey(string dateString, AwsConfiguration awsConfiguration)
        {
            var key = awsConfiguration.Credentials.SecretKey;
            var kSecret = Encoding.UTF8.GetBytes($"AWS4{key}");
            var kDate = HmacSha256(kSecret, dateString);
            var kRegion = HmacSha256(kDate, awsConfiguration.Region);
            var kService = HmacSha256(kRegion, awsConfiguration.Service);
            return HmacSha256(kService, "aws4_request");
        }

        static string CalculateHmacHex(byte[] key, string data)
        {
            var hash = HmacSha256(key, data);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }

        static byte[] HmacSha256(byte[] key, string data)
        {
            using (HMACSHA256 hmac = new HMACSHA256(key))
            {
                return hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
            }
        }

        static string CanonicalizeHeaderNames(IDictionary<string, string> headers)
        {
            var headersToSign = new List<string>(headers.Keys);
            headersToSign.Sort(StringComparer.OrdinalIgnoreCase);

            var sb = new StringBuilder();
            foreach (var header in headersToSign)
            {
                if (sb.Length > 0)
                    sb.Append(";");
                sb.Append(header.ToLower());
            }
            return sb.ToString();
        }

        static string CanonicalizeHeaders(IDictionary<string, string> headers)
        {
            var canonicalHeaders = new StringBuilder();
            var sortedHeaders = new SortedDictionary<string, string>(headers, StringComparer.OrdinalIgnoreCase);
            foreach (var header in sortedHeaders)
            {
                canonicalHeaders.Append($"{header.Key.ToLowerInvariant()}:{header.Value.Trim()}\n");
            }
            return canonicalHeaders.ToString();
        }
    }
}
