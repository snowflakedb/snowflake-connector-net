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
            var canonicalQueryString = CanonicalizeQueryString(uri.Query);
            var canonicalRequest = $"{httpMethod}\n{uri.AbsolutePath}\n{canonicalQueryString}\n{canonicalHeaders}\n{signedHeaders}\n{CalculateHash("")}";
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

        /// <summary>
        /// Canonicalizes the query string per AWS SigV4 spec:
        /// sort parameters alphabetically by name and normalize percent encoding to uppercase.
        /// </summary>
        static string CanonicalizeQueryString(string query)
        {
            if (string.IsNullOrEmpty(query))
                return string.Empty;
            if (query.StartsWith("?"))
                query = query.Substring(1);
            if (string.IsNullOrEmpty(query))
                return string.Empty;
            var parts = query.Split('&');
            Array.Sort(parts, StringComparer.Ordinal);
            return NormalizePercentEncoding(string.Join("&", parts));
        }

        /// <summary>
        /// Normalizes percent-encoded characters to uppercase hex as required by AWS SigV4.
        /// For example, %3a becomes %3A.
        /// </summary>
        static string NormalizePercentEncoding(string value)
        {
            var sb = new StringBuilder(value.Length);
            for (int i = 0; i < value.Length; i++)
            {
                if (value[i] == '%' && i + 2 < value.Length)
                {
                    sb.Append('%');
                    sb.Append(char.ToUpperInvariant(value[i + 1]));
                    sb.Append(char.ToUpperInvariant(value[i + 2]));
                    i += 2;
                }
                else
                {
                    sb.Append(value[i]);
                }
            }
            return sb.ToString();
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
