using Mono.Unix;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using System.Security;
namespace Snowflake.Data.Logger
{
    internal class EasyLoggerValidator
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFCredentialManagerFileImpl>();

        private readonly UnixOperations _unixOperations;

        public static readonly EasyLoggerValidator Instance = new EasyLoggerValidator(UnixOperations.Instance);

        internal EasyLoggerValidator(UnixOperations unixOperations)
        {
            _unixOperations = unixOperations;
        }

        internal void ValidateLogFilePermissions(UnixStream stream)
        {
            ValidateUserAndGroupPermissions(stream);
            if ((stream.FileAccessPermissions & ~EasyLoggingStarter.Instance._logFileUnixPermissions) != 0)
                ThrowSecurityException("Attempting to read or write to log file with too broad permissions assigned");
        }

        internal void ValidateUserAndGroupPermissions(UnixStream stream)
        {
            if (stream.OwnerUser.UserId != _unixOperations.GetCurrentUserId())
                ThrowSecurityException("Attempting to read or write a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != _unixOperations.GetCurrentGroupId())
                ThrowSecurityException("Attempting to read or write a file not owned by the effective group of the current process");
        }

        private void ThrowSecurityException(string errorMessage)
        {
            s_logger.Error(errorMessage);
            throw new SecurityException(errorMessage);
        }
    }
}
