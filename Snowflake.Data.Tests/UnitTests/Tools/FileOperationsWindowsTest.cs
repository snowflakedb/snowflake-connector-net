using System.IO;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture, NonParallelizable]
    [Platform("Win")]
    public class FileOperationsWindowsTest
    {
        private static FileOperations s_fileOperations;
        private static readonly string s_relativeWorkingDirectory = $"file_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_content = "random text";
        private static readonly string s_fileName = "testfile";

        [SetUp]
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_fileOperations = new FileOperations();
        }

        [TearDown]
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        public void TestReadAllTextOnWindows()
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);

            // act
            var result = s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.AreEqual(s_content, result);
        }

        [Test]
        public void TestFileIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            File.Create(absoluteFilePath).Close();

            // act and assert
            Assert.IsTrue(s_fileOperations.IsFileSafe(absoluteFilePath));
        }
    }
}

