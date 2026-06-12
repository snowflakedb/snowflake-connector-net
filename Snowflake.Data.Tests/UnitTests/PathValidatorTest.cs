using System.IO;
using System.Security;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using Snowflake.Data.Tests.Util;
using NUnit.Framework;

namespace Snowflake.Data.Tests.UnitTests 
{

    [TestFixture]
    public sealed class PathValidatorTest
    {
        [Test]
        public void TestValidateFileDestinationPath_SimpleFileName_DoesNotThrow()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            PathValidator.ValidateFileDestinationPath(baseDirectory, "data.csv");
        }

        [Test]
        public void TestValidateFileDestinationPath_FileNameWithSubdirectory_DoesNotThrow()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            var destinationRelativePath = GetPath("subdir/data.csv");
            PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath);
        }

        [Test]
        public void TestValidateFileDestinationPath_RelativeNavigation_Throws()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            var destinationRelativePath = GetPath("../etc/someDir");
            var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
            Assert.That(ex.Message, Contains.Substring("resolves outside the target directory"));
        }

        [Test]
        public void TestValidateFileDestinationPath_DeepRelativeNavigation_Throws()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            var destinationRelativePath = GetPath("../../etc/shadow");
            var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
            Assert.That(ex.Message, Contains.Substring("resolves outside the target directory"));
        }

        [Test]
        public void TestValidateFileDestinationPath_NavigationWithinPath_Throws()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            var destinationRelativePath = GetPath("subdir/../../etc/someDir");
            var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
            Assert.That(ex.Message, Contains.Substring("resolves outside the target directory"));
        }

        [Test]
        public void TestValidateFileDestinationPath_AbsolutePathOutsideBase_Throws()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            var destinationRelativePath = GetPath("../etc/someDir");
            Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
        }

        [Test]
        public void TestValidateFileDestinationPath_BaseDirectoryWithTrailingSeparator_DoesNotThrow()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            PathValidator.ValidateFileDestinationPath(baseDirectory, "data.csv");
        }

        [Test]
        public void TestValidateFileDestinationPath_SiblingDirectoryPrefix_Throws()
        {
            // Ensure "/tmp/foo" does not match "/tmp/foobar/file.txt"
            var baseDirectory = GetPath("/tmp/foo");
            var destinationRelativePath = GetPath("../foobar/file.txt");
            var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
            Assert.That(ex.Message, Contains.Substring("resolves outside the target directory"));
        }

        [Test]
        public void TestValidateFileDestinationPath_DotFileName_DoesNotThrow()
        {
            var baseDirectory = GetPath("/tmp/downloads");
            PathValidator.ValidateFileDestinationPath(baseDirectory, ".hidden_file");
        }

        private static string GetPath(string path) => path.Replace('/', Path.DirectorySeparatorChar);
    }
}
