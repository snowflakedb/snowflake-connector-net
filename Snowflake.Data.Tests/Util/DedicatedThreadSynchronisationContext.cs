using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;

namespace Snowflake.Data.Tests.Util
{
    /*
     * This class will not deadlock, but it will cause tests to fail if the Send or Post methods have been called during a test
     */
    public sealed class MockSynchronizationContext : SynchronizationContext
    {
        int callCount = 0;

        public override void Post(SendOrPostCallback d, object state)
        {
            callCount++;
            base.Post(d, state);
        }

        public override void Send(SendOrPostCallback d, object state)
        {
            callCount++;
            base.Send(d, state);
        }

        public static void SetupContext()
        {
            SynchronizationContext.SetSynchronizationContext(new MockSynchronizationContext());
        }

        public static void Verify()
        {
            MockSynchronizationContext ctx = (MockSynchronizationContext)SynchronizationContext.Current;
            Assert.Zero(ctx.callCount, "MockSynchronizationContext was called - this can cause deadlock. Make sure ConfigureAwait(false) is used in every await point in the library");
            SynchronizationContext.SetSynchronizationContext(null);
        }
    }

    /*
     * This can be used to test what happens when a library metod is called from a SyncronizationContext.
     * If there are any deadlocks in the code, this will trigger the deadlock.
     * 
     * Usage: 
     *      DedicatedThreadSynchronisationContext.RunInContext(_ => TestSimpleLargeResultSet());
     * 
     */
    public sealed class DedicatedThreadSynchronisationContext : SynchronizationContext, IDisposable
    {
        public DedicatedThreadSynchronisationContext()
        {
            m_thread = new Thread(ThreadWorkerDelegate);
            m_thread.Start(this);
        }

        public void Dispose()
        {
            m_queue.CompleteAdding();
        }

        /// <summary>Dispatches an asynchronous message to the synchronization context.</summary>
        /// <param name="d">The System.Threading.SendOrPostCallback delegate to call.</param>
        /// <param name="state">The object passed to the delegate.</param>
        public override void Post(SendOrPostCallback d, object state)
        {
            if (d == null) throw new ArgumentNullException("d");
            m_queue.Add(new KeyValuePair<SendOrPostCallback, object>(d, state));
        }

        /// <summary> As 
        public override void Send(SendOrPostCallback d, object state)
        {
            using (var handledEvent = new ManualResetEvent(false))
            {
                Post(SendOrPostCallback_BlockingWrapper, Tuple.Create(d, state, handledEvent));
                handledEvent.WaitOne();
            }
        }

        public int WorkerThreadId { get { return m_thread.ManagedThreadId; } }

        // This will run the callback in a synchronizationContext that is equivalent to a GUI or ASP.Net program 
        // If there are any async problems in the method, this code will provoke the deadlock.
        public static void RunInContext(SendOrPostCallback d)
        {
            using (var ctx = new DedicatedThreadSynchronisationContext())
            {
                ctx.Send(d, null);
            }
        }

        //=========================================================================================

        private static void SendOrPostCallback_BlockingWrapper(object state)
        {
            var innerCallback = (state as Tuple<SendOrPostCallback, object, ManualResetEvent>);
            try
            {
                innerCallback.Item1(innerCallback.Item2);
            }
            finally
            {
                innerCallback.Item3.Set();
            }
        }

        /// <summary>The queue of work items.</summary>
        private readonly BlockingCollection<KeyValuePair<SendOrPostCallback, object>> m_queue =
            new BlockingCollection<KeyValuePair<SendOrPostCallback, object>>();

        private readonly Thread m_thread = null;

        /// <summary>Runs a loop to process all queued work items.</summary>
        private void ThreadWorkerDelegate(object obj)
        {
            SynchronizationContext.SetSynchronizationContext(obj as SynchronizationContext);

            try
            {
                foreach (var workItem in m_queue.GetConsumingEnumerable())
                    workItem.Key(workItem.Value);
            }
            catch (ObjectDisposedException) { }
        }
    }
}
