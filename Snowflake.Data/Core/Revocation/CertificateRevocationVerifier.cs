using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Org.BouncyCastle.Asn1.X509;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Revocation
{
    internal class CertificateRevocationVerifier
    {
        private readonly CertRevocationCheckMode _certRevocationCheckMode;
        private readonly bool _enableCRLDiskCaching;
        private readonly bool _enableCRLInMemoryCaching;
        private readonly bool _allowCertificatesWithoutCrlUrl;
        private readonly TimeProvider _timeProvider;
        private readonly IRestRequester _restRequester;
        private readonly CertificateCrlDistributionPointsExtractor _crlExtractor;
        private readonly CrlParser _crlParser;
        private readonly CrlRepository _crlRepository;

        private static readonly ConcurrentDictionary<string, object> s_locksForCrlUrls = new ConcurrentDictionary<string, object>();
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<CertificateRevocationVerifier>();

        public CertificateRevocationVerifier(
            HttpClientConfig config,
            TimeProvider timeProvider,
            IRestRequester restRequester,
            CertificateCrlDistributionPointsExtractor crlExtractor,
            CrlParser crlParser,
            CrlRepository crlRepository)
        {
            _certRevocationCheckMode = config.CertRevocationCheckMode;
            _enableCRLDiskCaching = config.EnableCRLDiskCaching;
            _enableCRLInMemoryCaching = config.EnableCRLInMemoryCaching;
            _allowCertificatesWithoutCrlUrl = config.AllowCertificatesWithoutCrlUrl;
            _timeProvider = timeProvider;
            _restRequester = restRequester;
            _crlExtractor = crlExtractor;
            _crlParser = crlParser;
            _crlRepository = crlRepository;
        }

        public bool CertificateValidationCallback(HttpRequestMessage _, X509Certificate2 certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            bool defaultValidationPassed = sslPolicyErrors == SslPolicyErrors.None;
            if (!defaultValidationPassed)
                return false;
            return CheckCertificateRevocationStatus(certificate, chain);
        }

        private bool CheckCertificateRevocationStatus(X509Certificate2 certificate, X509Chain chain)
        {
            if (_certRevocationCheckMode == CertRevocationCheckMode.Disabled)
                return true; // OPEN
            var result = CheckChainRevocation(chain);
            if (result == ChainRevocationCheckResult.ChainUnrevoked)
                return true; // OPEN
            var unsuccessfulResults = new HashSet<ChainRevocationCheckResult> { result };
            foreach (var alternativeChain in FindAlternativeValidChains(certificate, chain))
            {
                var alternativeResult = CheckChainRevocation(alternativeChain);
                if (alternativeResult == ChainRevocationCheckResult.ChainUnrevoked)
                    return true; // OPEN
                unsuccessfulResults.Add(alternativeResult);
            }
            if (unsuccessfulResults.Contains(ChainRevocationCheckResult.ChainError) && _certRevocationCheckMode == CertRevocationCheckMode.Advisory)
            {
                s_logger.Debug("Encountered errors when checking revocation status for the certificate chain but assumed it is not revoked due to Advisory mode (fail open)");
                return true; // FAIL OPEN
            }
            return false; // CLOSE or FAIL CLOSE
        }

        private static object GetLock(string crlUrl) =>
            s_locksForCrlUrls.GetOrAdd(crlUrl, _ => new object());

        private ChainRevocationCheckResult CheckChainRevocation(X509Chain chain)
        {
            var chainResult = ChainRevocationCheckResult.ChainUnrevoked;
            var chainElementsCount = chain.ChainElements.Count;
            var index = 0;
            foreach (var chainElement in chain.ChainElements)
            {
                index++;
                var isRoot = index == chainElementsCount;
                if (isRoot)
                    continue;
                var certificate = chainElement.Certificate;
                var crlUrls = _crlExtractor.Extract(certificate);
                if (!ContainsAnyValue(crlUrls))
                {
                    if (_allowCertificatesWithoutCrlUrl)
                    {
                        s_logger.Debug("Certificate has missing CRL Distribution Point URLs");
                        continue;
                    }
                    chainResult = ChainRevocationCheckResult.ChainError;
                    continue;
                }
                var certStatus = CheckCertRevocation(certificate, crlUrls);
                if (certStatus == CertRevocationCheckResult.CertRevoked)
                {
                    chainResult = ChainRevocationCheckResult.ChainRevoked;
                    break;
                }
                if (certStatus == CertRevocationCheckResult.CertError)
                {
                    chainResult = ChainRevocationCheckResult.ChainError;
                }
            }
            return chainResult;
        }

        private bool ContainsAnyValue(string[] values)
        {
            if (values.Length == 0)
                return false;
            return values.Any(v => !string.IsNullOrEmpty(v));
        }

        internal CertRevocationCheckResult CheckCertRevocation(X509Certificate2 certificate, string[] crlUrls)
        {
            var results = new HashSet<CertRevocationCheckResult>();
            foreach (var crlUrl in crlUrls)
            {
                var result = CheckCertRevocationForOneCrlUrl(certificate, crlUrl);
                if (result == CertRevocationCheckResult.CertRevoked)
                {
                    return result; // fail fast
                }
                results.Add(result);
            }

            if (results.Contains(CertRevocationCheckResult.CertError))
            {
                return CertRevocationCheckResult.CertError;
            }
            return CertRevocationCheckResult.CertUnrevoked;
        }

        private CertRevocationCheckResult CheckCertRevocationForOneCrlUrl(X509Certificate2 certificate, string crlUrl)
        {
            Crl crl = null;
            var lockObject = GetLock(crlUrl);
            lock (lockObject)
            {
                var cachedCrl = _crlRepository.Get(crlUrl);
                var now = _timeProvider.UtcNow();
                var needsFreshCrl = cachedCrl == null || cachedCrl.NeedsFreshCrl(now);
                var shouldUpdateCrl = false;
                if (needsFreshCrl)
                {
                    var newCrl = FetchCrl(crlUrl);
                    shouldUpdateCrl = newCrl != null && (cachedCrl == null || newCrl.ThisUpdate > cachedCrl.ThisUpdate);
                    if (shouldUpdateCrl)
                    {
                        crl = newCrl;
                    }
                    else
                    {
                        if (cachedCrl != null && cachedCrl.NextUpdate > now)
                        {
                            crl = cachedCrl;
                        }
                        else
                        {
                            s_logger.Error($"Unable to fetch a valid or newer CRL from {crlUrl}. No fallback available.");
                            return CertRevocationCheckResult.CertError;
                        }
                    }
                }
                else
                {
                    crl = cachedCrl;
                }
                if (!IsCrlSignatureAndIssuerValid(crl, certificate, crlUrl))
                {
                    s_logger.Error($"Unable to verify CRL for {crlUrl}");
                    return CertRevocationCheckResult.CertError;
                }
                if (shouldUpdateCrl)
                {
                    _crlRepository.Set(crlUrl, crl);
                }
            }
            if (crl.IsRevoked(certificate.SerialNumber))
            {
                return CertRevocationCheckResult.CertRevoked;
            }
            return CertRevocationCheckResult.CertUnrevoked;
        }

        private bool IsCrlSignatureAndIssuerValid(Crl crl, X509Certificate2 certificate, string crlUrl) =>
            // TODO: signature verification will be done later
            IsIssuerEquivalent(crl, certificate) && IsIssuerDistributionPointValid(crl, crlUrl);

        private bool IsIssuerDistributionPointValid(Crl crl, string crlUrl)
        {
            var crlIdps = crl.IssuerDistributionPoints;
            if (crlIdps.Length == 0)
                return true;
            return crlIdps.Any(crlUrl.Equals);
        }

        internal bool IsIssuerEquivalent(Crl crl, X509Certificate2 certificate)
        {
            var issuerNameFromCert = new X509Name(FixSInIssuerName(certificate.IssuerName.Name));
            var issuerFromCrl = new X509Name(crl.IssuerName);
            return issuerNameFromCert.Equivalent(issuerFromCrl);
        }

        private string FixSInIssuerName(string issuerName)
        {
            // TODO: figure out how to do it better
            if (issuerName.StartsWith("S="))
                return "ST=" + issuerName.Substring(2);
            return issuerName.Replace(", S=", ", ST=");
        }

        private Crl FetchCrl(string crlUrl)
        {
            var timeout = TimeSpan.FromSeconds(60);
            var request = new HttpRequestMessage(HttpMethod.Get, crlUrl);
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, timeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, timeout);
            byte[] crlBytes = null;
            DateTime now;
            try
            {
                var response = _restRequester.Get(new RestRequestWrapper(request));
                now = _timeProvider.UtcNow();
                crlBytes = response.Content.ReadAsByteArrayAsync().Result;
            }
            catch (Exception exception)
            {
                s_logger.Error($"Downloading crl from {crlUrl} failed", exception);
                return null;
            }
            if (crlBytes == null || crlBytes.Length == 0)
            {
                s_logger.Error($"Downloading crl from {crlUrl} failed");
                return null;
            }
            try
            {
                var crl = _crlParser.Parse(crlBytes, now);
                if (crl == null)
                    s_logger.Error($"Parsing crl failed for the crl downloaded from: {crlUrl}");
                return crl;
            }
            catch (Exception exception)
            {
                s_logger.Error($"Parsing crl failed for the crl downloaded from: {crlUrl}", exception);
                return null;
            }
        }

        private List<X509Chain> FindAlternativeValidChains(X509Certificate2 certificate, X509Chain chain)
        {
            return new List<X509Chain>(); // TODO: build alternative chains
        }
    }
}
