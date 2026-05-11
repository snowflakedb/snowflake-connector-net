using System;
using System.IO;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class FileOperationsWindowsTest : IDisposable
    {
        public FileOperationsWindowsTest()
        {
            Before();
        }

        private static FileOperations s_fileOperations;
        private static readonly string s_relativeWorkingDirectory = $"file_operations_test_{Path.GetRandomFileName()}";
        private static readonly string s_workingDirectory = Path.Combine(TempUtil.GetTempPath(), s_relativeWorkingDirectory);
        private static readonly string s_content = "random text";
        private static readonly string s_fileName = "testfile";
        public static void Before()
        {
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }

            s_fileOperations = new FileOperations();
        }
        public static void After()
        {
            Directory.Delete(s_workingDirectory, true);
        }

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestReadAllTextOnWindows()
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);

            // act
            var result = s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(s_content, result);
        }

        [FactRunOnPlatform(FactRunOnPlatformAttribute.KnownOSPlatform.Windows)]
        public void TestFileIsSafeOnWindows()
        {
            // arrange
            var absoluteFilePath = Path.Combine(s_workingDirectory, s_fileName);
            File.Create(absoluteFilePath).Close();

            // act and assert
            Assert.True(s_fileOperations.IsFileSafe(absoluteFilePath));
        }
    
        public void Dispose()
        {
            After();
        }
}
}

