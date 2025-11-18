using System.IO;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture, NonParallelizable]
    [Platform("Win")]
    public class DirectoryOperationsWindowsTest
    {
        private static DirectoryOperations s_directoryOperations;
        private static readonly string s_relativeWorkingDirectory = $"directory_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_dirName = "testdir";

        [SetUp]
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_directoryOperations = new DirectoryOperations();
        }

        [TearDown]
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        public void TestDirectoryIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_dirName);
            Directory.CreateDirectory(absoluteFilePath);

            // act and assert
            Assert.IsTrue(s_directoryOperations.IsDirectorySafe(absoluteFilePath));
        }

        [Test]
        public void TestShouldCreateDirectoryWithSafePermissions()
        {
            // arrange
            var dirAbsolutePath = Path.Combine(s_workingDirectory, s_dirName);

            // act
            s_directoryOperations.CreateDirectory(dirAbsolutePath);

            // assert
            Assert.IsTrue(Directory.Exists(dirAbsolutePath));
            Assert.IsTrue(s_directoryOperations.IsDirectorySafe(dirAbsolutePath));
        }
    }
}
