namespace Snowflake.Data.Core.Session
{
    internal class FixedZeroCounter : ICounter
    {
        public int Count() => 0;

        public void Increase()
        {
        }

        public void Decrease()
        {
        }

        public void Reset()
        {
        }
    }
}
