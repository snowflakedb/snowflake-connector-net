using System.Threading;
using Xunit;

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
            Assert.Equal(0, ctx.callCount);
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
}
