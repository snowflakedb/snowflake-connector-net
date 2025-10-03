using System.Runtime.InteropServices;
using Mono.Unix;
using Snowflake.Data.Log;
using System.Security;

namespace Snowflake.Data.Core.Tools
{
    internal class ValidatorOperations
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ValidatorOperations>();

        private readonly long _unixUserId;

        private readonly long _unixGroupId;

        public static readonly ValidatorOperations Instance = new ValidatorOperations(UnixOperations.Instance);

        internal ValidatorOperations(UnixOperations unixOperations)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                _unixUserId = 0;
                _unixGroupId = 0;
            }
            else
            {
                _unixUserId = unixOperations.GetCurrentUserId();
                _unixGroupId = unixOperations.GetCurrentGroupId();
            }
        }

        internal void ValidateUserAndGroupPermissions(UnixStream stream)
        {
            if (stream.OwnerUser.UserId != _unixUserId)
                ThrowSecurityException("Attempting to read or write a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != _unixGroupId)
                ThrowSecurityException("Attempting to read or write a file not owned by the effective group of the current process");
        }

        internal void ThrowSecurityException(string errorMessage)
        {
            s_logger.Error(errorMessage);
            throw new SecurityException(errorMessage);
        }
    }
}
