using System.IO;
using System.Security;
using Snowflake.Data.Core.FileTransfer.StorageClient;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class PathValidatorTest
{
    [SFFact]
    public void TestValidateFileDestinationPath_SimpleFileName_DoesNotThrow()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        PathValidator.ValidateFileDestinationPath(baseDirectory, "data.csv");
    }

    [SFFact]
    public void TestValidateFileDestinationPath_FileNameWithSubdirectory_DoesNotThrow()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        var destinationRelativePath = GetPath("subdir/data.csv");
        PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath);
    }

    [SFFact]
    public void TestValidateFileDestinationPath_RelativeNavigation_Throws()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        var destinationRelativePath = GetPath("../etc/someDir");
        var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
        Assert.Contains("resolves outside the target directory", ex.Message);
    }

    [SFFact]
    public void TestValidateFileDestinationPath_DeepRelativeNavigation_Throws()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        var destinationRelativePath = GetPath("../../etc/shadow");
        var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
        Assert.Contains("resolves outside the target directory", ex.Message);
    }

    [SFFact]
    public void TestValidateFileDestinationPath_NavigationWithinPath_Throws()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        var destinationRelativePath = GetPath("subdir/../../etc/someDir");
        var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
        Assert.Contains("resolves outside the target directory", ex.Message);
    }

    [SFFact]
    public void TestValidateFileDestinationPath_AbsolutePathOutsideBase_Throws()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        var destinationRelativePath = GetPath("../etc/someDir");
        Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
    }

    [SFFact]
    public void TestValidateFileDestinationPath_BaseDirectoryWithTrailingSeparator_DoesNotThrow()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        PathValidator.ValidateFileDestinationPath(baseDirectory, "data.csv");
    }

    [SFFact]
    public void TestValidateFileDestinationPath_SiblingDirectoryPrefix_Throws()
    {
        // Ensure "/tmp/foo" does not match "/tmp/foobar/file.txt"
        var baseDirectory = GetPath("/tmp/foo");
        var destinationRelativePath = GetPath("../foobar/file.txt");
        var ex = Assert.Throws<SecurityException>(() => PathValidator.ValidateFileDestinationPath(baseDirectory, destinationRelativePath));
        Assert.Contains("resolves outside the target directory", ex.Message);
    }

    [SFFact]
    public void TestValidateFileDestinationPath_DotFileName_DoesNotThrow()
    {
        var baseDirectory = GetPath("/tmp/downloads");
        PathValidator.ValidateFileDestinationPath(baseDirectory, ".hidden_file");
    }

    private static string GetPath(string path) => path.Replace('/', Path.DirectorySeparatorChar);
}
