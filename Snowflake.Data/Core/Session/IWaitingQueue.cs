using System.Threading;

namespace Snowflake.Data.Core.Session
{
    public interface IWaitingQueue
    {
        bool Wait(int millisecondsTimeout, CancellationToken cancellationToken);

        void OnResourceIncrease();

        void OnResourceDecrease();

        bool IsAnyoneWaiting();

        bool IsWaitingEnabled();

        long GetWaitingTimeoutMillis();
        
        void SetWaitingTimeout(long timeoutMillis);
    }
}
