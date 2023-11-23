using System;
using System.Threading;

namespace Snowflake.Data.Core.Session
{
    public class SemaphoreBasedQueue: IWaitingQueue
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0, 1000); // TODO: how not to set it? 
        private readonly object _lock = new object();
        private int _waitingCount = 0;
        private long _waitingTimeoutMillis = 30000; // 30 seconds as default

        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken)
        {
            lock (_lock)
            {
                _waitingCount++;
            }
            try
            {
                return _semaphore.Wait(millisecondsTimeout, cancellationToken);
            }
            catch (OperationCanceledException exception)
            {
                return false;
            }
            finally
            {
                lock (_lock)
                {
                    _waitingCount--;
                }
            }
        }

        public void OnResourceIncrease()
        {
            _semaphore.Release(1);
        }

        public void OnResourceDecrease()
        {
            _semaphore.Wait(0, CancellationToken.None);
        }

        public bool IsAnyoneWaiting() => _waitingCount > 0;

        public bool IsWaitingEnabled() => true;

        public long GetWaitingTimeoutMillis() => _waitingTimeoutMillis;

        public void SetWaitingTimeout(long timeoutMillis) => _waitingTimeoutMillis = timeoutMillis;
    }
}
