using System;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Configuration
{
    internal class ClientFeatureFlags
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ClientFeatureFlags>();
        public bool IsEnabledExperimentalAuthentication { get; set; }

        public static readonly ClientFeatureFlags Instance = new ClientFeatureFlags(EnvironmentOperations.Instance);

        internal const string EnabledExperimentalAuthenticationVariableName = "SF_ENABLE_EXPERIMENTAL_AUTHENTICATION";
        private const bool EnabledExperimentalAuthenticationDefaultValue = false;

        internal ClientFeatureFlags(EnvironmentOperations environmentOperations)
        {
            IsEnabledExperimentalAuthentication = ReadEnabledExperimentalAuthentication(environmentOperations);
        }

        public void VerifyIfExperimentalAuthenticationEnabled(string authenticator)
        {
            if (!IsEnabledExperimentalAuthentication)
            {
                var exception = new SnowflakeDbException(SFError.EXPERIMENTAL_AUTHENTICATION_DISABLED, authenticator);
                s_logger.Error(exception.Message);
                throw exception;
            }
        }

        private bool ReadEnabledExperimentalAuthentication(EnvironmentOperations environmentOperations)
        {
            try
            {
                var isEnabledString = environmentOperations.GetEnvironmentVariable(EnabledExperimentalAuthenticationVariableName);
                if (string.IsNullOrEmpty(isEnabledString))
                {
                    s_logger.Debug($"Variable '{EnabledExperimentalAuthenticationVariableName}' not set. Using the default value: {EnabledExperimentalAuthenticationDefaultValue}");
                    return EnabledExperimentalAuthenticationDefaultValue;
                }
                var isEnabled = bool.Parse(isEnabledString);
                s_logger.Debug($"Variable '{EnabledExperimentalAuthenticationVariableName}' was read as: {isEnabled}");
                return isEnabled;
            }
            catch (Exception exception)
            {
                s_logger.Error($"Could not get or parse '{EnabledExperimentalAuthenticationVariableName}' variable. Used the default value: {EnabledExperimentalAuthenticationDefaultValue}.", exception);
                return EnabledExperimentalAuthenticationDefaultValue;
            }
        }
    }
}
