using System.Security.Authentication;

namespace Snowflake.Data.Core
{
    public static class SslProtocolsExtensions
    {
        public const SslProtocols Tls13 = (SslProtocols)12288;
        public const SslProtocols EnforcedTlsProtocols = SslProtocols.Tls12 | Tls13;
    }
}
