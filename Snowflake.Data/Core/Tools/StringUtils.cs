/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Core.Tools
{
    using System;
    using System.Security.Cryptography;

    public static class StringUtils
    {
        internal static string ToSha256Hash(this string text)
        {
            if (string.IsNullOrEmpty(text))
                return string.Empty;

            using (var sha = new SHA256Managed())
            {
                return BitConverter.ToString(sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(text))).Replace("-", string.Empty);
            }
        }
    }
}
