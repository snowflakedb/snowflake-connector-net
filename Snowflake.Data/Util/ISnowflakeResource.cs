using System;

namespace Snowflake.Data.Util
{
    public delegate void SnowflakeResourceEventHandler(ISnowflakeResource sender, EventArgs e);

    public interface ISnowflakeResource : IDisposable
    {
        event SnowflakeResourceEventHandler Disposed;

        string ResourceID { get; }
    }
}
