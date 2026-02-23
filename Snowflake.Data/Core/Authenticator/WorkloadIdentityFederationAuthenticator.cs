using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.WorkflowIdentity;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Core.Authenticator
{
    class WorkloadIdentityFederationAuthenticator : BaseAuthenticator, IAuthenticator
    {
        public const string AuthName = "workload_identity";
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkloadIdentityFederationAuthenticator>();

        private readonly EnvironmentOperations _environmentOperations;
        private readonly TimeProvider _timeProvider;
        private readonly AwsSdkWrapper _awsSdkWrapper;
        private readonly string _metadataHost;

        private AttestationProvider? _provider;
        private string _entraResource;
        private string _token;
        private string _impersonationPath;
        private WorkloadIdentityAttestationData _attestationData;

        public WorkloadIdentityFederationAuthenticator(SFSession session) : this(session, EnvironmentOperations.Instance, TimeProvider.Instance, AwsSdkWrapper.Instance, null)
        {
        }

        internal WorkloadIdentityFederationAuthenticator(
            SFSession session,
            EnvironmentOperations environmentOperations,
            TimeProvider timeProvider,
            AwsSdkWrapper awsSdkWrapper,
            string metadataHost) : base(session, AuthName)
        {
            _environmentOperations = environmentOperations;
            _timeProvider = timeProvider;
            _awsSdkWrapper = awsSdkWrapper;
            _metadataHost = metadataHost;
            if (session.properties.TryGetValue(SFSessionProperty.WORKLOAD_IDENTITY_PROVIDER, out var provider) && !string.IsNullOrEmpty(provider))
            {
                _provider = (AttestationProvider)Enum.Parse(typeof(AttestationProvider), provider, true);
            }
            else
            {
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, $"Property {SFSessionProperty.WORKLOAD_IDENTITY_PROVIDER} is required for Workload Identity authentication");
            }
            if (!session.properties.TryGetValue(SFSessionProperty.WORKLOAD_IDENTITY_ENTRA_RESOURCE, out _entraResource))
            {
                _entraResource = null;
            }
            if (!session.properties.TryGetValue(SFSessionProperty.WORKLOAD_IMPERSONATION_PATH, out _impersonationPath))
            {
                _impersonationPath = null;
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
            _attestationData = CreateAttestation();
            await LoginAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Authenticate()
        {
            _attestationData = CreateAttestation();
            Login();
        }

        internal WorkloadIdentityAttestationData CreateAttestation()
        {
            try
            {
                return _provider switch
                {
                    AttestationProvider.AWS => new WorkflowIdentityAwsAttestationRetriever(_environmentOperations, _timeProvider, _awsSdkWrapper, session.restRequester)
                        .CreateAttestationData(_entraResource, _token, _impersonationPath),
                    AttestationProvider.AZURE => new WorkflowIdentityAzureAttestationRetriever(_environmentOperations, session.restRequester, _metadataHost)
                        .CreateAttestationData(_entraResource, _token, _impersonationPath),
                    AttestationProvider.GCP => new WorkflowIdentityGcpAttestationRetriever(session.restRequester, _metadataHost)
                        .CreateAttestationData(_entraResource, _token, _impersonationPath),
                    AttestationProvider.OIDC => new WorkflowIdentityOidcAttestationRetriever()
                        .CreateAttestationData(_entraResource, _token, _impersonationPath),
                    _ => throw new SnowflakeDbException(SFError.WIF_ATTESTATION_ERROR, $"Unsupported attestation provider: {_provider}"),
                };
            }
            catch (Exception e) when (!(e is SnowflakeDbException))
            {
                var errorMessage = $"Failed to create attestation data for provider {_provider}: {e.Message}";
                s_logger.Error(errorMessage);
                throw new SnowflakeDbException(e, SFError.WIF_ATTESTATION_ERROR, new object[] { _provider, e.Message });
            }
        }
    }
}
