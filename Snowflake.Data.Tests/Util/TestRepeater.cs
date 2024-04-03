using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Tests.Util
{
    public class TestRepeater<T>
    {
        private readonly List<T> _result;

        private TestRepeater(List<T> result)
        {
            _result = result;
        }

        public void ForEach(Action<T> action) => _result.ForEach(action);
        
        public TestRepeater<T> SkipLargest<TKey>(Func<T, TKey> keySelector)
        {
            var resultsWithoutLargest = _result.OrderBy(keySelector).SkipLast(1).ToList();
            return new TestRepeater<T>(resultsWithoutLargest);
        }
        
        public static TestRepeater<T> Test(int times, Func<T> testFunction)
        {
            var resultList = Enumerable.Repeat(0, times)
                .Select(_ => testFunction())
                .ToList();
            return new TestRepeater<T>(resultList);
        }
    }
}
