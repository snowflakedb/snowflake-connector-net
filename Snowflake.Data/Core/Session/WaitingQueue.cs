using System;
using System.Collections.Generic;
using System.Threading;

namespace Snowflake.Data.Core.Session
{
    internal class WaitingQueue : IWaitingQueue
    {
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
        private readonly List<SemaphoreSlim> _queue = new List<SemaphoreSlim>();
        private readonly HashSet<SemaphoreSlim> _notSuccessfulCollection = new HashSet<SemaphoreSlim>();

        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken)
        {
            var semaphore = new SemaphoreSlim(0, 1);
            _lock.EnterWriteLock();
            try
            {
                _queue.Add(semaphore);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
            try
            {
                var waitingResult = semaphore.Wait(millisecondsTimeout, cancellationToken);
                bool shouldFail;
                _lock.EnterReadLock();
                try
                {
                    shouldFail = _notSuccessfulCollection.Contains(semaphore);
                }
                finally
                {
                    _lock.ExitReadLock();
                }
                if (shouldFail)
                {
                    _lock.EnterWriteLock();
                    try
                    {
                        _notSuccessfulCollection.Remove(semaphore);
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                    return false;
                }
                return waitingResult;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            finally
            {
                bool removed;
                _lock.EnterWriteLock();
                try
                {
                    removed = _queue.Remove(semaphore);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
                if (!removed && semaphore.CurrentCount > 0) // that means that it was removed by OnResourceIncrease() and not consumed by this waiting because of timeout
                {
                    OnResourceIncrease();
                }
            }
        }

        public void OnResourceIncrease()
        {
            SemaphoreSlim semaphore = null;
            _lock.EnterWriteLock();
            try
            {
                if (_queue.Count > 0)
                {
                    semaphore = _queue[0];
                    _queue.RemoveAt(0);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
            semaphore?.Release();
        }

        public bool IsAnyoneWaiting() => WaitingCount() > 0;

        public int WaitingCount()
        {
            _lock.EnterReadLock();
            try
            {
                return _queue.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public bool IsWaitingEnabled() => true;

        public void Reset()
        {
            _lock.EnterWriteLock();
            try
            {
                while (_queue.Count > 0)
                {
                    var semaphore = _queue[0];
                    _queue.RemoveAt(0);
                    _notSuccessfulCollection.Add(semaphore);
                    semaphore?.Release();
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
    }
}
