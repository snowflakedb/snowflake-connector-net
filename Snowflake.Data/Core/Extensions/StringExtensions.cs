using System;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Extensions;

internal static class StringExtensions
{
    public static string ToMaskedString(this Uri url) => url?.ToString().ToMaskedString();

    public static string ToMaskedString(this string input)
    {
        var maskSecrets = SecretDetector.Instance.MaskSecrets(input);
        return string.IsNullOrEmpty(maskSecrets.ErrStr)
            ? maskSecrets.MaskedText
            : $"[Error occurred during secret masking]: {maskSecrets.ErrStr}";
    }
}
