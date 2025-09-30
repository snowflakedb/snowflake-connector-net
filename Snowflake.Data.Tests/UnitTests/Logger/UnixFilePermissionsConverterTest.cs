using Mono.Unix;
using NUnit.Framework;
using Snowflake.Data.Log;
using System.Collections.Generic;

namespace Snowflake.Data.Tests.UnitTests.Logger
{
    [TestFixture]
    public class UnixFilePermissionsConverterTest
    {
        [Test]
        public void TestConversionForAllPermissionCombinations(
            [ValueSource(nameof(UserPermissionTestCases))] PermissionTestCase userTestCase,
            [ValueSource(nameof(GroupPermissionTestCases))] PermissionTestCase groupTestCase,
            [ValueSource(nameof(OtherPermissionTestCases))] PermissionTestCase otherTestCase)
        {
            // arrange
            var permissions = userTestCase.permissions | groupTestCase.permissions | otherTestCase.permissions;
            var expectedPermissions = userTestCase.expectedPermissions + groupTestCase.expectedPermissions + otherTestCase.expectedPermissions;

            // act
            var convertedPermissions = UnixFilePermissionsConverter.ConvertFileAccessPermissionsToInt(permissions);

            // assert
            Assert.AreEqual(expectedPermissions, convertedPermissions);
        }

        public static IEnumerable<PermissionTestCase> UserPermissionTestCases()
        {
            var noPermissionTestCase = new PermissionTestCase()
            {
                expectedPermissions = 000
            };

            var readPermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserRead,
                expectedPermissions = 400
            };

            var writePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserWrite,
                expectedPermissions = 200
            };

            var executePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserExecute,
                expectedPermissions = 100
            };

            var readWritePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserWrite,
                expectedPermissions = 600
            };

            var readExecutePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserRead | FileAccessPermissions.UserExecute,
                expectedPermissions = 500
            };

            var writeExecutePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserWrite | FileAccessPermissions.UserExecute,
                expectedPermissions = 300
            };

            var allPermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.UserReadWriteExecute,
                expectedPermissions = 700
            };

            return new[]
            {
                noPermissionTestCase,
                readPermissionsTestCase,
                writePermissionsTestCase,
                executePermissionsTestCase,
                readWritePermissionsTestCase,
                readExecutePermissionsTestCase,
                writeExecutePermissionsTestCase,
                allPermissionsTestCase
            };
        }

        public static IEnumerable<PermissionTestCase> GroupPermissionTestCases()
        {
            var noPermissionTestCase = new PermissionTestCase()
            {
                expectedPermissions = 000
            };

            var readPermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupRead,
                expectedPermissions = 040
            };

            var writePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupWrite,
                expectedPermissions = 020
            };

            var executePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupExecute,
                expectedPermissions = 010
            };

            var readWritePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupRead | FileAccessPermissions.GroupWrite,
                expectedPermissions = 060
            };

            var readExecutePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupRead | FileAccessPermissions.GroupExecute,
                expectedPermissions = 050
            };

            var writeExecutePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupWrite | FileAccessPermissions.GroupExecute,
                expectedPermissions = 030
            };

            var allPermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.GroupReadWriteExecute,
                expectedPermissions = 070
            };

            return new[]
            {
                noPermissionTestCase,
                readPermissionsTestCase,
                writePermissionsTestCase,
                executePermissionsTestCase,
                readWritePermissionsTestCase,
                readExecutePermissionsTestCase,
                writeExecutePermissionsTestCase,
                allPermissionsTestCase
            };
        }

        public static IEnumerable<PermissionTestCase> OtherPermissionTestCases()
        {
            var noPermissionTestCase = new PermissionTestCase()
            {
                expectedPermissions = 000
            };

            var readPermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherRead,
                expectedPermissions = 004
            };

            var writePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherWrite,
                expectedPermissions = 002
            };

            var executePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherExecute,
                expectedPermissions = 001
            };

            var readWritePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherRead | FileAccessPermissions.OtherWrite,
                expectedPermissions = 006
            };

            var readExecutePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherRead | FileAccessPermissions.OtherExecute,
                expectedPermissions = 005
            };

            var writeExecutePermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherWrite | FileAccessPermissions.OtherExecute,
                expectedPermissions = 003
            };

            var allPermissionsTestCase = new PermissionTestCase()
            {
                permissions = FileAccessPermissions.OtherReadWriteExecute,
                expectedPermissions = 007
            };

            return new[]
            {
                noPermissionTestCase,
                readPermissionsTestCase,
                writePermissionsTestCase,
                executePermissionsTestCase,
                readWritePermissionsTestCase,
                readExecutePermissionsTestCase,
                writeExecutePermissionsTestCase,
                allPermissionsTestCase
            };
        }

        public class PermissionTestCase
        {
            internal FileAccessPermissions permissions;
            internal int expectedPermissions;
        }
    }
}
