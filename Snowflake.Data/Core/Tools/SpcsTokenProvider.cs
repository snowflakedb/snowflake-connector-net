using System;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal sealed class SpcsTokenProvider
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SpcsTokenProvider>();
        private const string SpcsTokenPath = "/snowflake/session/spcs_token";

        internal static SpcsTokenProvider CreateIfRunningInSpcs() => CreateIfRunningInSpcs(EnvironmentFacade.Instance);

        internal static SpcsTokenProvider CreateIfRunningInSpcs(IEnvironmentFacade environmentFacade) =>
            string.IsNullOrEmpty(environmentFacade.GetString(EnvVars.RunningInsideSpcs))
                ? null
                : new SpcsTokenProvider(FileOperations.Instance, environmentFacade);

        private readonly FileOperations _fileOperations;
        private readonly IEnvironmentFacade _environmentFacade;

        internal SpcsTokenProvider(FileOperations fileOperations, IEnvironmentFacade environmentFacade)
        {
            _fileOperations = fileOperations;
            _environmentFacade = environmentFacade;
        }

        /// <summary>
        /// Returns the SPCS token read from /snowflake/session/spcs_token, or null.
        /// Only attempts to read the token when SNOWFLAKE_RUNNING_INSIDE_SPCS is set.
        /// Any I/O errors or missing/empty files are treated as "no token".
        /// </summary>
        internal string GetSpcsToken()
        {
            var tokenPath = _environmentFacade.GetString(EnvVars.RunningInsideSpcs);
            if (string.IsNullOrEmpty(tokenPath))
                return null;

            try
            {
                var token = _fileOperations.ReadAllText(SpcsTokenPath).Trim();
                return string.IsNullOrEmpty(token) ? null : token;
            }
            catch (Exception e)
            {
                s_logger.Warn($"Failed to read SPCS token from {tokenPath}: {e.Message}");
                return null;
            }
        }
    }
}
