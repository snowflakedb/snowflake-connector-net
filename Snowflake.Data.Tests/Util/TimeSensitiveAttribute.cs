using NUnit.Framework;

namespace Snowflake.Data.Tests.Util
{
    /// <summary>
    /// Marks a test as time-sensitive (e.g. uses Stopwatch to assert timing).
    /// Inherits NonParallelizable to prevent flakiness from CPU contention.
    /// </summary>
    public sealed class TimeSensitiveAttribute : ParallelizableAttribute
    {
        public string Rationale { get; set; }

        public TimeSensitiveAttribute() : base(ParallelScope.None)
        {

        }
    }
}
