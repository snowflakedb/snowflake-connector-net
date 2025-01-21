/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Security.Cryptography;

namespace Snowflake.Data.Core.Tools
{
    public static class StringUtils
    {
        internal static string ToSha256Hash(this string text)
        {
            if (string.IsNullOrEmpty(text))
                return string.Empty;

            using (var sha256Encoder = SHA256.Create())
            {
                var sha256Hash = sha256Encoder.ComputeHash(System.Text.Encoding.UTF8.GetBytes(text));
                return BitConverter.ToString(sha256Hash).Replace("-", string.Empty);
            }
        }
    }
}
