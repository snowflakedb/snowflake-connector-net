
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    internal interface IAuthenticator
    {
        void Authenticate();

        Task AuthenticateAsync(CancellationToken cancellationToken);
    }

}