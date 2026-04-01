using System;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal class SpcsTokenProvider
    {
        internal const string RunningInsideSpcsEnvVar = "SNOWFLAKE_RUNNING_INSIDE_SPCS";
        internal const string DefaultSpcsTokenPath = "/snowflake/session/spcs_token";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SpcsTokenProvider>();

        public static readonly SpcsTokenProvider Instance = new SpcsTokenProvider();

        private readonly FileOperations _fileOperations;
        private readonly EnvironmentOperations _environmentOperations;

        internal SpcsTokenProvider() : this(FileOperations.Instance, EnvironmentOperations.Instance)
        {
        }

        internal SpcsTokenProvider(FileOperations fileOperations, EnvironmentOperations environmentOperations)
        {
            _fileOperations = fileOperations;
            _environmentOperations = environmentOperations;
        }

        /// <summary>
        /// Returns the SPCS token read from /snowflake/session/spcs_token, or null.
        /// Only attempts to read the token when SNOWFLAKE_RUNNING_INSIDE_SPCS is set.
        /// Any I/O errors or missing/empty files are treated as "no token".
        /// </summary>
        internal virtual string GetSpcsToken()
        {
            if (string.IsNullOrEmpty(_environmentOperations.GetEnvironmentVariable(RunningInsideSpcsEnvVar)))
            {
                return null;
            }

            try
            {
                var token = _fileOperations.ReadAllText(DefaultSpcsTokenPath).Trim();
                return string.IsNullOrEmpty(token) ? null : token;
            }
            catch (Exception e)
            {
                s_logger.Warn($"Failed to read SPCS token from {DefaultSpcsTokenPath}: {e.Message}");
                return null;
            }
        }
    }
}
