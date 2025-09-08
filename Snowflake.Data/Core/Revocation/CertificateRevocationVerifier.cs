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
        private readonly bool _allowCertificatesWithoutCrlUrl;
        private readonly TimeProvider _timeProvider;
        private readonly IRestRequester _restRequester;
        private readonly CertificateCrlDistributionPointsExtractor _crlExtractor;
        private readonly CrlParser _crlParser;
        private readonly CrlRepository _crlRepository;

        private static readonly ConcurrentDictionary<string, object> s_locksForCrlUrls = new ConcurrentDictionary<string, object>();
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<CertificateRevocationVerifier>();
        private static readonly TimeSpan s_httpTimeout = TimeSpan.FromSeconds(60);

        public CertificateRevocationVerifier(
            HttpClientConfig config,
            TimeProvider timeProvider,
            IRestRequester restRequester,
            CertificateCrlDistributionPointsExtractor crlExtractor,
            CrlParser crlParser,
            CrlRepository crlRepository)
        {
            _certRevocationCheckMode = config.CertRevocationCheckMode;
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
            var joinedChainSubjects = GetJoinedChainSubjects(chain);
            s_logger.Debug($"Checking revocation status for certificate: '{certificate.Subject}' with the best chain of: {joinedChainSubjects}");
            if (_certRevocationCheckMode == CertRevocationCheckMode.Disabled)
            {
                s_logger.Debug($"Certificate revocation status checking is disabled. Allowing to use the certificate: '{certificate.Subject}'");
                return true; // OPEN
            }
            var result = CheckChainRevocation(chain);
            s_logger.Debug($"Revocation status for certificate: '{certificate.Subject}' with the best chain of: '{joinedChainSubjects}' is: {result.ToString()}");
            if (result == ChainRevocationCheckResult.ChainUnrevoked)
                return true; // OPEN
            var unsuccessfulResults = new HashSet<ChainRevocationCheckResult> { result };
            foreach (var alternativeChain in FindAlternativeValidChains(certificate, chain))
            {
                var alternativeChainSubjects = GetJoinedChainSubjects(alternativeChain);
                s_logger.Debug($"Checking revocation status for certificate: '{certificate.Subject}' with alternative chain of: '{alternativeChainSubjects}'");
                var alternativeResult = CheckChainRevocation(alternativeChain);
                s_logger.Debug($"Revocation status for certificate: '{certificate.Subject}' with the alternative chain of: '{alternativeChainSubjects}' is: {alternativeResult.ToString()}");
                if (alternativeResult == ChainRevocationCheckResult.ChainUnrevoked)
                    return true; // OPEN
                unsuccessfulResults.Add(alternativeResult);
            }
            if (unsuccessfulResults.Contains(ChainRevocationCheckResult.ChainError) && _certRevocationCheckMode == CertRevocationCheckMode.Advisory)
            {
                s_logger.Debug($"Encountered errors when checking revocation status for the certificate chains for certificate: '{certificate.Subject}'. Allowing to accept the certificate due to Advisory mode (fail open)");
                return true; // FAIL OPEN
            }
            return false; // CLOSE or FAIL CLOSE
        }

        private static string GetJoinedChainSubjects(X509Chain chain)
        {
            var chainSubjects = chain.ChainElements.Cast<X509ChainElement>().Select(e => e.Certificate.Subject);
            return string.Join(" -> ", chainSubjects);
        }

        private static object GetLock(string crlUrl) =>
            s_locksForCrlUrls.GetOrAdd(crlUrl, _ => new object());

        internal ChainRevocationCheckResult CheckChainRevocation(X509Chain chain)
        {
            var chainSubjects = GetJoinedChainSubjects(chain);
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
                if (IsShortLived(certificate))
                {
                    s_logger.Debug($"Skipping certificate revocation check for a short-lived certificate: {certificate.Subject} on position: {index} in chain: '{chainSubjects}'");
                    continue;
                }
                s_logger.Debug($"Checking certificate revocation status for certificate: {certificate.Subject} on position: {index} in chain: '{chainSubjects}'");
                var crlUrls = _crlExtractor.Extract(certificate);
                if (!ContainsAnyValue(crlUrls))
                {
                    if (_allowCertificatesWithoutCrlUrl)
                    {
                        s_logger.Debug($"Certificate '{certificate.Subject}' on position: {index} in chain: '{chainSubjects}' has missing CRL Distribution Point URLs but it is acceptable");
                        continue;
                    }
                    s_logger.Debug($"Certificate '{certificate.Subject}' on position: {index} in chain: '{chainSubjects}' has missing CRL Distribution Point URLs so it adds a {ChainRevocationCheckResult.ChainError.ToString()} to chain revocation status");
                    chainResult = ChainRevocationCheckResult.ChainError;
                    continue;
                }
                var joinedCrlUrls = string.Join(", ", crlUrls);
                s_logger.Debug($"Certificate '{certificate.Subject}' on position: {index} in chain: '{chainSubjects}' has following CRL Distribution Point URLs: {joinedCrlUrls}");
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
            s_logger.Debug($"Certificate chain '{chainSubjects}' revocation status is: {chainResult.ToString()}");
            return chainResult;
        }

        internal bool IsShortLived(X509Certificate2 certificate) =>
            IsShortLived(certificate.NotBefore.ToUniversalTime(), certificate.NotAfter.ToUniversalTime());

        private bool IsShortLived(DateTime notBeforeUtc, DateTime notAfterUtc)
        {
            // see Short-lived Subscriber Certificate section in: https://cabforum.org/working-groups/server/baseline-requirements/requirements/
            if (notBeforeUtc < new DateTime(2024, 3, 15, 0, 0, 0, DateTimeKind.Utc))
            {
                return false;
            }
            var maxDaysValidityPeriod = notBeforeUtc < new DateTime(2026, 3, 15, 0, 0, 0, DateTimeKind.Utc) ? 10 : 7;
            var firstNotAfterWhenCertIsNotShortLived = notBeforeUtc
                .AddHours(maxDaysValidityPeriod * 24) // adding hours not to bother about how adding days could be implemented
                .AddMinutes(1); // fix for inclusion start and end time
            return notAfterUtc < firstNotAfterWhenCertIsNotShortLived;
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
                    s_logger.Debug($"Certificate revocation status for certificate '{certificate.Subject}' is {CertRevocationCheckResult.CertRevoked.ToString()} because for one of its CRL urls the status was {CertRevocationCheckResult.CertRevoked.ToString()}");
                    return result; // fail fast
                }
                results.Add(result);
            }

            if (results.Contains(CertRevocationCheckResult.CertError))
            {
                s_logger.Debug($"Certificate revocation status for certificate '{certificate.Subject}' is {CertRevocationCheckResult.CertError.ToString()} because for one of its CRL urls the status was {CertRevocationCheckResult.CertError.ToString()} and there was no {CertRevocationCheckResult.CertRevoked.ToString()} status");
                return CertRevocationCheckResult.CertError;
            }
            s_logger.Debug($"Certificate revocation status for certificate '{certificate.Subject}' is {CertRevocationCheckResult.CertUnrevoked.ToString()}");
            return CertRevocationCheckResult.CertUnrevoked;
        }

        private CertRevocationCheckResult CheckCertRevocationForOneCrlUrl(X509Certificate2 certificate, string crlUrl)
        {
            s_logger.Debug($"Checking certificate revocation for certificate: '{certificate.Subject}' with CRL url: '{crlUrl}'");
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
                            s_logger.Error($"Unable to fetch a valid or newer CRL from '{crlUrl}' for certificate: '{certificate.Subject}'. No fallback available. Certificate revocation status is: {CertRevocationCheckResult.CertError.ToString()}");
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
                    s_logger.Error($"Unable to verify CRL: '{crlUrl}' for certificate: '{certificate.Subject}' because of CRL and certificate details mismatch. Certificate revocation status is: {CertRevocationCheckResult.CertError.ToString()}");
                    return CertRevocationCheckResult.CertError;
                }
                if (shouldUpdateCrl)
                {
                    _crlRepository.Set(crlUrl, crl);
                }
            }
            if (crl.IsRevoked(certificate.SerialNumber))
            {
                s_logger.Debug($"Certificate '{certificate.Subject}' has been verified for CRL: '{crlUrl}' to have status: {CertRevocationCheckResult.CertRevoked.ToString()}");
                return CertRevocationCheckResult.CertRevoked;
            }
            s_logger.Debug($"Certificate '{certificate.Subject}' has been verified for CRL: '{crlUrl}' to have status: {CertRevocationCheckResult.CertUnrevoked.ToString()}");
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
            var request = new HttpRequestMessage(HttpMethod.Get, crlUrl);
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, s_httpTimeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, s_httpTimeout);
            byte[] crlBytes = null;
            DateTime now;
            try
            {
                var response = _restRequester.Get(new RestRequestWrapper(request));
                now = _timeProvider.UtcNow();
                crlBytes = response.Content?.ReadAsByteArrayAsync().Result;
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
