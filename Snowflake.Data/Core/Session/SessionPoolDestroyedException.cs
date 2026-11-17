using System;

namespace Snowflake.Data.Core.Session;

/// <summary>
/// Thrown when a session is requested from a pool that has been destroyed since it was looked up.
/// The caller should look the pool up again, which creates a new one.
/// </summary>
internal sealed class SessionPoolDestroyedException : Exception
{
    public SessionPoolDestroyedException() : base("Connection pool was destroyed while getting a session")
    {
    }
}
