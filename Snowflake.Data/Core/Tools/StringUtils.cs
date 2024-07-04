// <copyright file="StringUtils.cs" company="Snowflake Inc">
//         Copyright (c) 2019-2024 Snowflake Inc. All rights reserved.
//  </copyright>

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
