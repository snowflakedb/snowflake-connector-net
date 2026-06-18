using System.IO;

namespace Snowflake.Data.Tests.UnitTests.Revocation
{
    public abstract class RevocationTests
    {
        protected static readonly string s_crlPath = Path.Combine("crl");
        protected static readonly string s_digiCertCrlPath = Path.Combine(s_crlPath, "DigiCertGlobalG2TLSRSASHA2562020CA1-1.crl");
        protected static readonly string s_digiCertCertificatePath = Path.Combine(s_crlPath, "digicert_certificate.crt");
        protected static readonly string s_digiCertParentCertificatePath = Path.Combine(s_crlPath, "digicert_parent_certificate.crt");
        protected const string DigiCertIssuer = "C=US,O=DigiCert Inc,CN=DigiCert Global G2 TLS RSA SHA256 2020 CA1";
        protected const string DigiCertThisUpdateString = "2025-07-25T00:18:54.0000000Z";
        protected const string DigiCertNextUpdateString = "2025-08-01T00:18:54.0000000Z";
        protected const string DigiCertCrlUrl1 = "http://crl3.digicert.com/DigiCertGlobalG2TLSRSASHA2562020CA1-1.crl";
        protected const string DigiCertCrlUrl2 = "http://crl4.digicert.com/DigiCertGlobalG2TLSRSASHA2562020CA1-1.crl";
        protected const string DigiCertUnrevokedCertSerialNumber = "0C5B94A21CF8D834C17A58724F3EA385"; // pragma: allowlist secret
        protected const string DigiCertRevokedCertSerialNumber = "084E2808851C58174D0EF94B29571042"; // pragma: allowlist secret
    }
}
