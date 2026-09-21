using System;
using System.Security.Authentication;

namespace Snowflake.Data.Core
{
    public static class SslProtocolsExtensions
    {
        public const SslProtocols Tls13 = (SslProtocols)12288;

        public static SslProtocols FromString(string protocol)
        {
            return protocol.ToLower() switch
            {
                "tls12" => SslProtocols.Tls12,
                "tls13" => Tls13,
                _ => throw new ArgumentException($"Unsupported TLS protocol: {protocol}")
            };
        }

        // Tls13 is not a named member of SslProtocols on all supported targets, so ToString() on it
        // would render the raw numeric value in error messages.
        internal static string ToDisplayString(this SslProtocols protocol)
        {
            if (protocol == SslProtocols.Tls12)
                return "TLS12";
            if (protocol == Tls13)
                return "TLS13";
            if (protocol == (SslProtocols.Tls12 | Tls13))
                return "TLS12, TLS13";
            if (protocol == SslProtocols.None)
                return "system default";
            return protocol.ToString();
        }
    }
}
