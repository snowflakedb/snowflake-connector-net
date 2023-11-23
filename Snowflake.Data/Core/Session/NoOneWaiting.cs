using System.Threading;

namespace Snowflake.Data.Core.Session
{
    public class NoOneWaiting: IWaitingQueue
    {
        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken)
        {
            return false;
        }

        public void OnResourceIncrease()
        {
        }

        public void OnResourceDecrease()
        {
        }

        public bool IsAnyoneWaiting()
        {
            return false;
        }

        public bool IsWaitingEnabled()
        {
            return false;
        }

        public long GetWaitingTimeoutMillis() => 0;

        public void SetWaitingTimeout(long timeoutMillis)
        {
            throw new System.NotImplementedException();
        }
    }
}
