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
        private static readonly Regex s_awsKeyPattern = new(@"(aws_key_id|aws_secret_key|access_key_id|secret_access_key)('|"")?(\s*[:=]\s*)'([^']+)'", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_awsTokenPattern = new(@"(accessToken|tempToken|keySecret)\""\s*:\s*\""([a-z0-9/+]{32,}={0,2})\""", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_awsServerSidePattern = new(@"((x-amz-server-side-encryption)([a-z0-9\-])*)\s*(:|=)\s*([a-z0-9/_\-+:=])+", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_sasTokenPattern = new(@"(sig|signature|AWSAccessKeyId|password|passcode)=([a-z0-9%/+]{16,})", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_privateKeyPattern = new(@"-----BEGIN PRIVATE KEY-----\n([a-z0-9/+=\n]{32,})\n-----END PRIVATE KEY-----", RegexOptions.IgnoreCase | RegexOptions.Multiline); // pragma: allowlist secret
        private static readonly Regex s_privateKeyDataPattern = new(@"""privateKeyData"": ""([a-z0-9/+=\n]{10,})""", RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled);
        private static readonly Regex s_privateKeyPropertyPrefixPattern = new(@"(private_key\s*=)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_connectionTokenPattern = new(@"(token|assertion content)(['""\s:=]+)([a-z0-9=/_\-+:]{8,})", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_tokenPropertyPattern = new(@"(token)(\s*=)(.*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_passwordPattern = new(@"(password|passcode|client_?secret|pwd|proxypassword|private_key_pwd)(['""\s:=]+)([a-z0-9!""#$%&'\()*+,-./:;<=>?@\[\]\^_`{|}~]{6,})", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex s_passwordPropertyPattern = new(@"(password|passcode|oauthclientsecret|proxypassword|private_key_pwd)(\s*=)(.*)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static readonly Func<string, string>[] s_maskFunctions = {
            MaskAWSServerSide,
            MaskAWSKeys,
            MaskSASTokens,
            MaskAWSTokens,
            MaskPrivateKey,
            MaskPrivateKeyData,
            MaskPrivateKeyProperty,
            MaskPassword,
            MaskPasswordProperty,
            MaskConnectionTokens,
            MaskTokenProperty
        };

        private static string MaskAWSKeys(string text) => s_awsKeyPattern.Replace(text, @"$1$2$3'****'");

        private static string MaskAWSTokens(string text) => s_awsTokenPattern.Replace(text, @"$1"":""XXXX""");

        private static string MaskAWSServerSide(string text) => s_awsServerSidePattern.Replace(text, @"$1:....");

        private static string MaskSASTokens(string text) => s_sasTokenPattern.Replace(text, @"$1=****");

        private static string MaskPrivateKey(string text) => s_privateKeyPattern.Replace(text, "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----");

        private static string MaskPrivateKeyProperty(string text)
        {
            var match = s_privateKeyPropertyPrefixPattern.Match(text);
            if (match.Success)
            {
                int length = match.Index + match.Value.Length;
                return text.Substring(0, length) + "****";
            }
            return text;
        }

        private static string MaskPrivateKeyData(string text) => s_privateKeyDataPattern.Replace(text, @"""privateKeyData"": ""XXXX""");

        private static string MaskConnectionTokens(string text) => s_connectionTokenPattern.Replace(text, @"$1$2****");

        private static string MaskPassword(string text) => s_passwordPattern.Replace(text, @"$1$2****");

        private static string MaskPasswordProperty(string text) => s_passwordPropertyPattern.Replace(text, @"$1$2****");

        private static string MaskTokenProperty(string text) => s_tokenPropertyPattern.Replace(text, @"$1$2****");

        public static Mask MaskSecrets(string text)
        {
            Mask result = new Mask(maskedText: text);

            if (String.IsNullOrEmpty(text))
            {
                return result;
            }

            try
            {
                result.maskedText = MaskAllPatterns(text);
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

        private static string MaskAllPatterns(string text)
        {
            string result = text;
            foreach (var maskFunction in s_maskFunctions)
            {
                result = maskFunction.Invoke(result);
            }
            if (CUSTOM_PATTERNS_LENGTH > 0)
            {
                result = MaskCustomPatterns(result);
            }
            return result;
        }
    }
}
