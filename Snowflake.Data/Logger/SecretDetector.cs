/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Text.RegularExpressions;

namespace Snowflake.Data.Log
{
    class SecretDetector
    {
        public struct Mask
        {
            public Mask(bool isMasked = false, string maskedText = null, string errStr = null)
            {
                this.isMasked = isMasked;
                this.maskedText = maskedText;
                this.errStr = errStr;
            }

            public bool isMasked { get; set; }
            public string maskedText { get; set; }
            public string errStr { get; set; }
        }

        private static readonly string AWS_KEY_PATTERN = @"(aws_key_id|aws_secret_key|access_key_id|secret_access_key)\s*=\s*'([^']+)'";
        private static readonly string AWS_TOKEN_PATTERN = @"(accessToken|tempToken|keySecret)\s*:\s*""([a-z0-9/+]{32,}={0,2})""";
        private static readonly string SAS_TOKEN_PATTERN = @"(sig|signature|AWSAccessKeyId|password|passcode)=(\?P<secret>[a-z0-9%/+]{16,})";
        private static readonly string PRIVATE_KEY_PATTERN = @"-----BEGIN PRIVATE KEY-----\n([a-z0-9/+=\n]{32,})\n-----END PRIVATE KEY-----";
        private static readonly string PRIVATE_KEY_DATA_PATTERN = @"""privateKeyData"": ""([a - z0 - 9 /+=\\n]{10,})""";
        private static readonly string CONNECTION_TOKEN_PATTERN = @"(token|assertion content)([\'\""\s:=]+)([a-z0-9=/_\-\+]{8,})";
        private static readonly string PASSWORD_PATTERN = @"(password|pwd)([\'\""\s:=]+)([a-z0-9!\""#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_`\{\|\}~]{8,})";

        private static string MaskAWSKeys(string text)
        {
            return Regex.Replace(text, AWS_KEY_PATTERN, @"$1='****'",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskAWSTokens(string text)
        {
            return Regex.Replace(text, AWS_TOKEN_PATTERN, @"$1"":""XXXX""",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskSASTokens(string text)
        {
            return Regex.Replace(text, SAS_TOKEN_PATTERN, @"$1=****",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskPrivateKey(string text)
        {
            return Regex.Replace(text, PRIVATE_KEY_PATTERN, "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----",
                                         RegexOptions.IgnoreCase | RegexOptions.Multiline);
        }

        private static string MaskPrivateKeyData(string text)
        {
            return Regex.Replace(text, PRIVATE_KEY_DATA_PATTERN, @"""privateKeyData"": ""XXXX""",
                                         RegexOptions.IgnoreCase | RegexOptions.Multiline);
        }

        private static string MaskConnectionTokens(string text)
        {
            return Regex.Replace(text, CONNECTION_TOKEN_PATTERN, @"$1$2****",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskPassword(string text)
        {
            return Regex.Replace(text, PASSWORD_PATTERN, @"$1$2****",
                                         RegexOptions.IgnoreCase);
        }

        public static Mask MaskSecrets(string text)
        {
            Mask result = new Mask(maskedText: text);

            if (String.IsNullOrEmpty(text))
            {
                return result;
            }

            try
            {
                result.maskedText =
                    MaskConnectionTokens(
                        MaskPassword(
                            MaskPrivateKeyData(
                                MaskPrivateKey(
                                    MaskAWSTokens(
                                        MaskSASTokens(
                                            MaskAWSKeys(text)))))));

                if (result.maskedText != text)
                {
                    result.isMasked = true;
                }
            }
            catch (Exception ex)
            {
                //We'll assume that the exception was raised during masking
                //to be safe consider that the log has sensitive information
                //and do not raise an exception.
                result.isMasked = true;
                result.maskedText = ex.Message;
                result.errStr = ex.Message;
            }
            return result;
        }
    }
}
