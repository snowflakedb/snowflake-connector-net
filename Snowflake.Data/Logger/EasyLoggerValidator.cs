using Mono.Unix;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Logger
{
    internal class EasyLoggerValidator
    {
        public static readonly EasyLoggerValidator Instance = new EasyLoggerValidator();

        internal void ValidateLogFilePermissions(UnixStream stream)
        {
            ValidatorOperations.Instance.ValidateUserAndGroupPermissions(stream);
            if ((stream.FileAccessPermissions & ~EasyLoggingStarter.Instance._logFileUnixPermissions) != 0)
                ValidatorOperations.Instance.ThrowSecurityException("Attempting to read or write to log file with too broad permissions assigned");
        }
    }
}
