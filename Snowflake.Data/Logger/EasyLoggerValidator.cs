using Mono.Unix;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using System;

namespace Snowflake.Data.Logger
{
    internal class EasyLoggerValidator
    {
        public static readonly EasyLoggerValidator Instance = new EasyLoggerValidator();

        internal void ValidateLogFilePermissions(UnixStream stream)
        {
            ValidatorOperations.Instance.ValidateUserAndGroupPermissions(stream);
            Console.WriteLine($"stream.FileAccessPermissions: {stream.FileAccessPermissions}");
            Console.WriteLine($"EasyLoggingStarter.Instance._logFileUnixPermissions: {EasyLoggingStarter.Instance._logFileUnixPermissions}");
            Console.WriteLine($"(stream.FileAccessPermissions & ~EasyLoggingStarter.Instance._logFileUnixPermissions): {(stream.FileAccessPermissions & ~EasyLoggingStarter.Instance._logFileUnixPermissions)}");
            if ((stream.FileAccessPermissions & ~EasyLoggingStarter.Instance._logFileUnixPermissions) != 0)
                ValidatorOperations.Instance.ThrowSecurityException("Attempting to read or write to log file with too broad permissions assigned");
        }
    }
}
