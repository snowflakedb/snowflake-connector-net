using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class TimeSensitiveFixtureIT
    {
        [Theory]
        [MemberData(nameof(SFBaseTestCases))]
        public async Task SFBaseTest((Type, MethodInfo) args)
        {
            // Arrange
            var testClass = Activator.CreateInstance(args.Item1);
            var startupMethod = args.Item1.GetMethod("BeforeTest", BindingFlags.Public | BindingFlags.Instance);
            var tearDownMethod = args.Item1.GetMethod("AfterTest", BindingFlags.Public | BindingFlags.Instance);

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

        [Theory]
        [MemberData(nameof(SFBaseAsyncTestCases))]
        public async Task SFBaseAsyncTest((Type, MethodInfo) args)
        {
            // Arrange
            var testClass = Activator.CreateInstance(args.Item1);
            var startupMethod = args.Item1.GetMethod("BeforeTest", BindingFlags.Public | BindingFlags.Instance);
            var tearDownMethod = args.Item1.GetMethod("AfterTest", BindingFlags.Public | BindingFlags.Instance);

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

        public static object[][] SFBaseAsyncTestCases = GetTestCaseData(typeof(SFBaseTestAsync), typeof(SFBaseTest));
        public static object[][] SFBaseTestCases = GetTestCaseData(typeof(SFBaseTest));

        private static object[][] GetTestCaseData(Type assignableTo, Type notAssignableTo = null) => Assembly
            .GetExecutingAssembly()
            .GetTypes()
            .Where(x => assignableTo.IsAssignableFrom(x) && (notAssignableTo == null || !notAssignableTo.IsAssignableFrom(x)))
            .SelectMany(x => x.GetMethods(BindingFlags.Public | BindingFlags.Instance).Select(y => (Type: x, Method: y)))
            .Where(x => x.Method.GetCustomAttributes(typeof(TimeSensitiveAttribute), false).Any())
            .GroupBy(x => x.Method.Name)
            .SelectMany(x => x.Select(y => (Data: y, HasDuplicateName: x.Count() > 1)))
            .Select(x => new object[] { x.Data })
            .ToArray();
    }
}
