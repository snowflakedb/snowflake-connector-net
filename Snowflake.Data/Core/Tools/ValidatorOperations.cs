using Mono.Unix;
using Snowflake.Data.Log;
using System.Security;

namespace Snowflake.Data.Core.Tools
{
    internal class ValidatorOperations
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ValidatorOperations>();

        private readonly UnixOperations _unixOperations;

        public static readonly ValidatorOperations Instance = new ValidatorOperations(UnixOperations.Instance);

        internal ValidatorOperations(UnixOperations unixOperations)
        {
            _unixOperations = unixOperations;
        }

        internal void ValidateUserAndGroupPermissions(UnixStream stream)
        {
            if (stream.OwnerUser.UserId != _unixOperations.GetCurrentUserId())
                ThrowSecurityException("Attempting to read or write a file not owned by the effective user of the current process");
            if (stream.OwnerGroup.GroupId != _unixOperations.GetCurrentGroupId())
                ThrowSecurityException("Attempting to read or write a file not owned by the effective group of the current process");
        }

        internal void ThrowSecurityException(string errorMessage)
        {
            s_logger.Error(errorMessage);
            throw new SecurityException(errorMessage);
        }
    }
}
