using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    class WorkloadIdentityFederationAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AuthName = "workload_identity";
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkloadIdentityFederationAuthenticator>();

        private readonly ClientFeatureFlags _featureFlags;
        private readonly EnvironmentOperations _environmentOperations;
        private readonly TimeProvider _timeProvider;
        private readonly AwsSdkWrapper _awsSdkWrapper;
        private readonly string _metadataHost;

        private AttestationProvider? _provider;
        private string _entraResource;
        private string _token;
        private WorkloadIdentityAttestationData _attestationData;

        public WorkloadIdentityFederationAuthenticator(SFSession session) : this(session, ClientFeatureFlags.Instance, EnvironmentOperations.Instance, TimeProvider.Instance, AwsSdkWrapper.Instance, null)
        {
        }

        internal WorkloadIdentityFederationAuthenticator(
            SFSession session,
            ClientFeatureFlags featureFlags,
            EnvironmentOperations environmentOperations,
            TimeProvider timeProvider,
            AwsSdkWrapper awsSdkWrapper,
            string metadataHost) : base(session, AuthName)
        {
            _featureFlags = featureFlags;
            _environmentOperations = environmentOperations;
            _timeProvider = timeProvider;
            _awsSdkWrapper = awsSdkWrapper;
            _metadataHost = metadataHost;
            if (session.properties.TryGetValue(SFSessionProperty.WIFPROVIDER, out var provider) && !string.IsNullOrEmpty(provider))
            {
                _provider = (AttestationProvider)Enum.Parse(typeof(AttestationProvider), provider, true);
            }
            else
            {
                _provider = null;
            }
            if (!session.properties.TryGetValue(SFSessionProperty.WIFENTRARESOURCE, out _entraResource))
            {
                _entraResource = null;
            }
            session.properties.TryGetValue(SFSessionProperty.TOKEN, out _token);
        }

        public static bool IsWorkloadIdentityAuthenticator(string authenticator) =>
            AuthName.Equals(authenticator, StringComparison.InvariantCultureIgnoreCase);

        protected override void SetSpecializedAuthenticatorData(ref LoginRequestData data)
        {
            data.Token = _attestationData.Credential;
            data.Provider = _attestationData.Provider.ToString();
        }

        public async Task AuthenticateAsync(CancellationToken cancellationToken)
        {
            _featureFlags.VerifyIfAuthenticationEnabled(AuthName);
            _attestationData = CreateAttestation();
            await LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Authenticate()
        {
            _featureFlags.VerifyIfAuthenticationEnabled(AuthName);
            _attestationData = CreateAttestation();
            Login();
        }

        internal WorkloadIdentityAttestationData CreateAttestation()
        {
            return _provider switch
            {
                AttestationProvider.AWS => new WorkflowIdentityAwsAttestationRetriever(_environmentOperations, _timeProvider, _awsSdkWrapper)
                    .CreateAttestationData(_entraResource, _token),
                AttestationProvider.AZURE => new WorkflowIdentityAzureAttestationRetriever(_environmentOperations, session.restRequester, _metadataHost)
                    .CreateAttestationData(_entraResource, _token),
                AttestationProvider.GCP => new WorkflowIdentityGcpAttestationRetriever(session.restRequester, _metadataHost)
                    .CreateAttestationData(_entraResource, _token),
                AttestationProvider.OIDC => new WorkflowIdentityOidcAttestationRetriever()
                    .CreateAttestationData(_entraResource, _token),
                _ => AutodetectAttestation()
            };
        }

        private WorkloadIdentityAttestationData AutodetectAttestation()
        {
            var retrievers = new Func<WorkloadIdentityAttestationRetriever>[]
            {
                () => new WorkflowIdentityOidcAttestationRetriever(),
                () => new WorkflowIdentityAzureAttestationRetriever(_environmentOperations, session.restRequester, _metadataHost),
                () => new WorkflowIdentityAwsAttestationRetriever(_environmentOperations, _timeProvider, _awsSdkWrapper),
                () => new WorkflowIdentityGcpAttestationRetriever(session.restRequester, _metadataHost)
            };
            s_logger.Debug("Auto detection of attestations");
            foreach (var retrieverFunc in retrievers)
            {
                var retriever = retrieverFunc();
                try
                {
                    s_logger.Debug($"Trying to do attestation for {retriever.GetAttestationProvider().ToString()}");
                    var attestationData = retriever.CreateAttestationData(_entraResource, _token);
                    s_logger.Debug($"Attestation successfully created for {retriever.GetAttestationProvider().ToString()}");
                    return attestationData;
                }
                catch (Exception)
                {
                    s_logger.Debug($"Auto-detection failed for: {retriever.GetAttestationProvider()}");
                }
            }
            s_logger.Error("Auto detection of attestations failed");
            throw new SnowflakeDbException(SFError.WIF_ATTESTATION_ERROR, "AUTO DETECTION", "Could not receive attestation for any of attestation providers");
        }
    }
}
