/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// Interface for Authenticator
    /// For simplicity, only the Asynchronous function call is created
    /// </summary>
    internal interface IAuthenticator
    {
        /// <summary>
        /// Process the authentication asynchronously
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="SnowflakeDbException"></exception>
        Task AuthenticateAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Process the authentication synchronously
        /// </summary>
        /// <exception cref="SnowflakeDbException"></exception>
        void Authenticate();
    }
}
