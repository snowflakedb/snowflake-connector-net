using System;
using System.IO;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
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

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public void TestReadAllTextOnWindows()
        {
            var filePath = CreateConfigTempFile(s_workingDirectory, s_content);

            // act
            var result = s_fileOperations.ReadAllText(filePath, TomlConnectionBuilder.ValidateFilePermissions);

            // assert
            Assert.Equal(s_content, result);
        }

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public void TestFileCopyOnWindows()
        {
            // arrange
            var srcFilePath = Path.Combine(s_workingDirectory, "src_file");
            var dstFilePath = Path.Combine(s_workingDirectory, "dst_file");
            File.WriteAllText(srcFilePath, s_content);

            // act
            s_fileOperations.CopyFile(srcFilePath, dstFilePath);

            // assert
            Assert.True(File.Exists(dstFilePath));
            Assert.Equal(s_content, File.ReadAllText(dstFilePath));
        }

        [SFFact]
        public void TestFileCopyOverwritesExistingDestination()
        {
            // arrange
            var srcFilePath = Path.Combine(s_workingDirectory, "src_overwrite");
            var dstFilePath = Path.Combine(s_workingDirectory, "dst_overwrite");
            File.WriteAllText(srcFilePath, s_content);
            File.WriteAllText(dstFilePath, "old content");

            // act
            s_fileOperations.CopyFile(srcFilePath, dstFilePath);

            // assert
            Assert.Equal(s_content, File.ReadAllText(dstFilePath));
        }

        public void Dispose()
        {
            After();
        }
    }
}

