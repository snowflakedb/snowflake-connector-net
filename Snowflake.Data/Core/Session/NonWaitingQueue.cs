using System.Threading;

namespace Snowflake.Data.Core.Session
{
    internal class NonWaitingQueue : IWaitingQueue
    {
        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken)
        {
            return false;
        }

        public void OnResourceIncrease()
        {
        }

        public bool IsAnyoneWaiting()
        {
            return false;
        }

        public int WaitingCount()
        {
            return 0;
        }

        public bool IsWaitingEnabled()
        {
            return false;
        }

        public void Reset()
        {
        }
    }
}
