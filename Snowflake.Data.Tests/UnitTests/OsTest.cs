using System.Runtime.InteropServices;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class OsTest
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EnvironmentOperations>();

        [Test]
        public void TestOs()
        {
            // var environment = EnvironmentOperations.Instance;
            // var osVariable = environment.GetEnvironmentVariable("OS");
            // s_logger.Warn($"OS: {osVariable}!!!");
            // // s_logger.Warn($"OS Description: {RuntimeInformation.OSDescription}!!!");
            // s_logger.Warn($"OS RuntimeIdentifier: {RuntimeInformation.RuntimeIdentifier}!!!");
            s_logger.Warn($"OS OSArchitecture: {RuntimeInformation.OSArchitecture}!!!");
        }
    }
}
