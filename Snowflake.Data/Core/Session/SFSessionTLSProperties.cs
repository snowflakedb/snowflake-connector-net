namespace Snowflake.Data.Core.Session;

internal readonly record struct SFSessionTLSProperties(
    string MinTlsProtocol,
    string MaxTlsProtocol,
    bool TlsProtocolsExplicitlyRequested,
    TlsCipherPolicy TlsCipherPolicy)
{
    internal interface IExtractor
    {
        SFSessionTLSProperties ExtractProperties(SFSessionProperties propertiesDictionary);
    }

    internal sealed class Extractor : IExtractor
    {
        public SFSessionTLSProperties ExtractProperties(SFSessionProperties propertiesDictionary)
        {
            return new SFSessionTLSProperties(
                MinTlsProtocol: propertiesDictionary[SFSessionProperty.MINTLS],
                MaxTlsProtocol: propertiesDictionary[SFSessionProperty.MAXTLS],
                TlsProtocolsExplicitlyRequested: propertiesDictionary.TlsProtocolsExplicitlyRequested,
                TlsCipherPolicy: null);
        }
    }
}
