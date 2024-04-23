/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
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

        private static List<string> CUSTOM_PATTERNS_REGEX = new List<string>();
        private static List<string> CUSTOM_PATTERNS_MASK = new List<string>();
        private static int CUSTOM_PATTERNS_LENGTH;

        public static void SetCustomPatterns(string[] customRegex, string[] customMask)
        {
            if (CUSTOM_PATTERNS_LENGTH != 0)
            {
                ClearCustomPatterns();
            }

            if (customRegex.Length == customMask.Length)
            {
                CUSTOM_PATTERNS_LENGTH = customRegex.Length;
                for (int index = 0; index < CUSTOM_PATTERNS_LENGTH; index++)
                {
                    CUSTOM_PATTERNS_REGEX.Add(customRegex[index]);
                    CUSTOM_PATTERNS_MASK.Add(customMask[index]);
                }
            }
            else
            {
                throw new ArgumentException("Regex count and mask count must be equal.");
            }
        }

        public static void ClearCustomPatterns()
        {
            CUSTOM_PATTERNS_REGEX.Clear();
            CUSTOM_PATTERNS_MASK.Clear();
            CUSTOM_PATTERNS_LENGTH = 0;
        }

        private static string MaskCustomPatterns(string text)
        {
            string result;
            for (int index = 0; index < CUSTOM_PATTERNS_LENGTH; index++)
            {
                result = Regex.Replace(text, CUSTOM_PATTERNS_REGEX[index], CUSTOM_PATTERNS_MASK[index],
                                         RegexOptions.IgnoreCase);

                if (result != text)
                {
                    return result;
                }
            }
            return text;
        }

        /*
         * https://docs.microsoft.com/en-us/dotnet/standard/base-types/character-escapes-in-regular-expressions
         * . $ ^ { [ ( | ) * + ? \
         * The characters are special regular expression language elements. 
         * To match them in a regular expression, they must be escaped or included in a positive character group.
         * [ ] \ - ^
         * The characters are special character group element.
         * To match them in a character group, they must be escaped.
         */
        private static readonly string AWS_KEY_PATTERN = @"(aws_key_id|aws_secret_key|access_key_id|secret_access_key)('|"")?(\s*[:=]\s*)'([^']+)'";
        private static readonly string AWS_TOKEN_PATTERN = @"(accessToken|tempToken|keySecret)\""\s*:\s*\""([a-z0-9/+]{32,}={0,2})\""";
        private static readonly string AWS_SERVER_SIDE_PATTERN = @"((x-amz-server-side-encryption)([a-z0-9\-])*)\s*(:|=)\s*([a-z0-9/_\-+:=])+";
        private static readonly string SAS_TOKEN_PATTERN = @"(sig|signature|AWSAccessKeyId|password|passcode)=([a-z0-9%/+]{16,})";
        private static readonly string PRIVATE_KEY_PATTERN = @"-----BEGIN PRIVATE KEY-----\n([a-z0-9/+=\n]{32,})\n-----END PRIVATE KEY-----";
        private static readonly string PRIVATE_KEY_DATA_PATTERN = @"""privateKeyData"": ""([a-z0-9/+=\n]{10,})""";
        private static readonly string CONNECTION_TOKEN_PATTERN = @"(token|assertion content)(['""\s:=]+)([a-z0-9=/_\-+:]{8,})";
        private static readonly string PASSWORD_PATTERN = @"(password|passcode|pwd|proxypassword)(['""\s:=]+)([a-z0-9!""#$%&'\()*+,-./:;<=>?@\[\]\^_`{|}~]{6,})";

        private static string MaskAWSKeys(string text)
        {
            return Regex.Replace(text, AWS_KEY_PATTERN, @"$1$2$3'****'",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskAWSTokens(string text)
        {
            return Regex.Replace(text, AWS_TOKEN_PATTERN, @"$1"":""XXXX""",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskAWSServerSide(string text)
        {
            return Regex.Replace(text, AWS_SERVER_SIDE_PATTERN, @"$1:....",
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
                                            MaskAWSKeys(
                                                MaskAWSServerSide(text))))))));
                if (CUSTOM_PATTERNS_LENGTH > 0)
                {
                    result.maskedText = MaskCustomPatterns(result.maskedText);
                }
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
