using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [Parallelizable(ParallelScope.Children)]
    [Order(3)]
    public sealed class TimeSensitiveFixtureIT
    {
        [TestCaseSource(nameof(SFBaseTestCases))]
        [Retry(3)]
        public async Task SFBaseTest((Type, MethodInfo) args)
        {
            // Arrange
            var testClass = Activator.CreateInstance(args.Item1);
            var startupMethod = args.Item1.GetMethods(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(x => x.GetCustomAttribute<SetUpAttribute>() != null);
            var tearDownMethod = args.Item1.GetMethods(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(x => x.GetCustomAttribute<TearDownAttribute>() != null);

            try
            {
                startupMethod?.Invoke(testClass, null);
                var result = args.Item2.Invoke(testClass, null);
                await (result as Task ?? Task.CompletedTask);
            }
            finally
            {
                tearDownMethod?.Invoke(testClass, null);
            }
        }

        [TestCaseSource(nameof(SFBaseAsyncTestCases))]
        [Retry(3)]
        public async Task SFBaseAsyncTest((Type, MethodInfo) args)
        {
            // Arrange
            var testClass = Activator.CreateInstance(args.Item1);
            var startupMethod = args.Item1.GetMethods(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(x => x.GetCustomAttribute<SetUpAttribute>() != null);
            var tearDownMethod = args.Item1.GetMethods(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(x => x.GetCustomAttribute<TearDownAttribute>() != null);

            startupMethod?.Invoke(testClass, null);

            try
            {
                var result = args.Item2.Invoke(testClass, null);
                await (result as Task ?? Task.CompletedTask);
            }
            finally
            {
                tearDownMethod?.Invoke(testClass, null);
            }
        }

        public static TestCaseData[] SFBaseAsyncTestCases = GetTestCaseData(typeof(SFBaseTestAsync), typeof(SFBaseTest));
        public static TestCaseData[] SFBaseTestCases = GetTestCaseData(typeof(SFBaseTest));

        private static TestCaseData[] GetTestCaseData(Type assignableTo, Type notAssignableTo = null) => Assembly
            .GetExecutingAssembly()
            .GetTypes()
            .Where(x => assignableTo.IsAssignableFrom(x) && (notAssignableTo == null || !notAssignableTo.IsAssignableFrom(x)))
            .SelectMany(x => x.GetMethods(BindingFlags.Public | BindingFlags.Instance).Select(y => (x, y)))
            .Where(x => x.Item2.GetCustomAttributes(typeof(TimeSensitiveAttribute), false).Any())
            .Select(x => new TestCaseData(x).SetName(x.y.Name))
            .ToArray();
    }
}
