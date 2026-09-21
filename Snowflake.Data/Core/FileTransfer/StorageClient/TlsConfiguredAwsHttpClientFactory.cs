// Amazon.Runtime.HttpClientFactory does not exist in the AWS net472 assets: on .NET Framework the
// SDK runs on its HttpWebRequest pipeline, which has no HttpClient to replace. SFS3Client rejects a
// requested TLS restriction there instead.
#if !NETFRAMEWORK
using System.Net.Http;
using System.Security.Authentication;
using Amazon.Runtime;

namespace Snowflake.Data.Core.FileTransfer.StorageClient
{
    /// <summary>
    /// Supplies the AWS SDK with an HttpClient honoring the TLS protocols requested by
    /// MINTLS/MAXTLS. AmazonS3Config exposes no TLS setting, so replacing the client is the only
    /// way to constrain the protocols used for stage transfers.
    ///
    /// Once a factory is set the SDK stops applying ProxyHost, ProxyPort, GetWebProxy and
    /// AllowAutoRedirect from the config, so those are applied on the handler here instead.
    /// </summary>
    internal sealed class TlsConfiguredAwsHttpClientFactory : HttpClientFactory
    {
        private readonly SslProtocols _tlsProtocols;
        private readonly ProxyCredentials _proxyCredentials;

        internal TlsConfiguredAwsHttpClientFactory(SslProtocols tlsProtocols, ProxyCredentials proxyCredentials)
        {
            _tlsProtocols = tlsProtocols;
            _proxyCredentials = proxyCredentials;
        }

        public override HttpClient CreateHttpClient(IClientConfig clientConfig) =>
            HttpUtil.Instance.CreateStorageHttpClientShared(
                _tlsProtocols,
                _proxyCredentials,
                clientConfig.AllowAutoRedirect,
                clientConfig.MaxConnectionsPerServer);

        // The client is shared and lives for the process, so neither the SDK cache nor the SDK
        // disposal logic should take it over.
        public override bool UseSDKHttpClientCaching(IClientConfig clientConfig) => false;

        public override bool DisposeHttpClientsAfterUse(IClientConfig clientConfig) => false;

        public override string GetConfigUniqueString(IClientConfig clientConfig) =>
            HttpUtil.BuildStorageHandlerKey(
                _tlsProtocols,
                _proxyCredentials,
                clientConfig.AllowAutoRedirect,
                clientConfig.MaxConnectionsPerServer);
    }
}
#endif
