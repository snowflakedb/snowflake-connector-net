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
    }
}
