using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using Mono.Unix;
using Mono.Unix.Native;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;
using static Snowflake.Data.Tests.UnitTests.Configuration.EasyLoggingConfigGenerator;

namespace Snowflake.Data.Tests.Tools
{
    using System.Security;

    [TestFixture, NonParallelizable]
    public class UnixOperationsTest
    {
        private static UnixOperations s_unixOperations;
        private static readonly string s_workingDirectory = Path.Combine(Path.GetTempPath(), "easy_logging_test_configs_", Path.GetRandomFileName());

        [OneTimeSetUp]
        public static void BeforeAll()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;
            if (!Directory.Exists(s_workingDirectory))
            {
                Directory.CreateDirectory(s_workingDirectory);
            }
            s_unixOperations = new UnixOperations();
        }

        [OneTimeTearDown]
        public static void AfterAll()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;
            Directory.Delete(s_workingDirectory, true);
        }

        [Test]
        public void TestDetectGroupOrOthersWritablePermissions(
            [ValueSource(nameof(GroupOrOthersWritablePermissions))] FilePermissions groupOrOthersWritablePermissions,
            [ValueSource(nameof(GroupNotWritablePermissions))] FilePermissions groupNotWritablePermissions,
            [ValueSource(nameof(OtherNotWritablePermissions))] FilePermissions otherNotWritablePermissions)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            // arrange
            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            var readWriteUserPermissions = FilePermissions.S_IRUSR | FilePermissions.S_IWUSR;
            var filePermissions = readWriteUserPermissions | groupOrOthersWritablePermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, filePermissions);

            // act
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(filePath, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.IsTrue(result);
        }

        [Test]
        public void TestDetectGroupOrOthersNotWritablePermissions(
            [ValueSource(nameof(UserPermissions))] FilePermissions userPermissions,
            [ValueSource(nameof(GroupNotWritablePermissions))] FilePermissions groupNotWritablePermissions,
            [ValueSource(nameof(OtherNotWritablePermissions))] FilePermissions otherNotWritablePermissions)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }

            var filePath = CreateConfigTempFile(s_workingDirectory, "random text");
            var filePermissions = userPermissions | groupNotWritablePermissions | otherNotWritablePermissions;
            Syscall.chmod(filePath, filePermissions);

            // act
            var result = s_unixOperations.CheckFileHasAnyOfPermissions(filePath, FileAccessPermissions.GroupWrite | FileAccessPermissions.OtherWrite);

            // assert
            Assert.IsFalse(result);
        }

        [Test]
        public void TestReadAllTextCheckingPermissions()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);
            var filePermissions = FileAccessPermissions.UserReadWriteExecute;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act
            var result = s_unixOperations.ReadAllText(filePath);

            // assert
            Assert.AreEqual(content, result);
        }

        [Test]
        public void TestShouldThrowExceptionIfOtherPermissionsIsSetWhenReadAllText()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Ignore("skip test on Windows");
            }
            var content = "random text";
            var filePath = CreateConfigTempFile(s_workingDirectory, content);
            var filePermissions = FileAccessPermissions.UserReadWriteExecute | FileAccessPermissions.OtherReadWriteExecute;
            Syscall.chmod(filePath, (FilePermissions)filePermissions);

            // act and assert
            Assert.Throws<SecurityException>(() => s_unixOperations.ReadAllText(filePath), "Attempting to read a file with too broad permissions assigned");
        }

        public static IEnumerable<FilePermissions> UserPermissions()
        {
            yield return FilePermissions.S_IRUSR;
            yield return FilePermissions.S_IWUSR;
            yield return FilePermissions.S_IXUSR;
            yield return FilePermissions.S_IRUSR | FilePermissions.S_IWUSR | FilePermissions.S_IXUSR;
        }

        public static IEnumerable<FilePermissions> GroupOrOthersWritablePermissions()
        {
            yield return FilePermissions.S_IWGRP;
            yield return FilePermissions.S_IWOTH;
            yield return FilePermissions.S_IWGRP | FilePermissions.S_IWOTH;
        }

        public static IEnumerable<FilePermissions> GroupNotWritablePermissions()
        {
            yield return 0;
            yield return FilePermissions.S_IRGRP;
            yield return FilePermissions.S_IXGRP;
            yield return FilePermissions.S_IRGRP | FilePermissions.S_IXGRP;
        }

        public static IEnumerable<FilePermissions> OtherNotWritablePermissions()
        {
            yield return 0;
            yield return FilePermissions.S_IROTH;
            yield return FilePermissions.S_IXOTH;
            yield return FilePermissions.S_IROTH | FilePermissions.S_IXOTH;
        }
    }
}
