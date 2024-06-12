using System;

namespace Snowflake.Data.Core.Session
{
    internal class NonNegativeCounter : ICounter
    {
        private int _value;

        public int Count() => _value;

        public void Increase() => _value++;

        public void Decrease()
        {
            _value = Math.Max(_value - 1, 0);
        }

        public void Reset() => _value = 0;
    }
}
