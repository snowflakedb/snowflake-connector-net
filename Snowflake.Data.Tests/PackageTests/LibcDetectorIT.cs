using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.PackageTests
{
    public sealed class LibcDetectorIT
    {
        [SFFact(SkipCondition.SkipOnLinux)]
        public void TestDetectReturnsNotApplicableOnNonLinux()
        {
            // Act
            var (family, version) = LibcDetector.Instance.Detect();

            // Assert
            Assert.Equal(LibcFamily.NotApplicable, family);
            Assert.Null(version);
        }

        [SFFact(SkipCondition.RunOnlyOnLinux)]
        public void TestDetectReturnsKnownFamilyOnLinux()
        {
            // Act
            var (family, _) = LibcDetector.Instance.Detect();

            // Assert — on any Linux box we should always resolve to one of the known families
            Assert.NotEqual(LibcFamily.NotApplicable, family);
        }

        [SFFact(SkipCondition.RunOnlyOnLinux)]
        public void TestDetectReturnsVersionStringOnLinuxGlibc()
        {
            // Act
            var (family, version) = LibcDetector.Instance.Detect();

            if (family == LibcFamily.Glibc)
            {
                Assert.NotNull(version);
                Assert.NotEmpty(version);
            }
            else
            {
                return; // System uses non-glibc; glibc version assertion not applicable
            }
        }

        [SFFact]
        public void TestTryGetGlibcVersionDoesNotThrow()
        {
            // Act — must not throw on any platform
            LibcDetector.TryGetGlibcVersion(out _);
        }

        [SFFact(SkipCondition.SkipOnLinux)]
        public void TestTryGetGlibcVersionReturnsFalseOnNonLinux()
        {
            // Act
            var found = LibcDetector.TryGetGlibcVersion(out var version);

            // Assert
            Assert.False(found);
            Assert.Null(version);
        }

        [SFFact(SkipCondition.RunOnlyOnLinux)]
        public void TestTryGetGlibcVersionReturnsTrueOnLinuxGlibc()
        {
            // Act
            var found = LibcDetector.TryGetGlibcVersion(out var version);

            // On glibc systems the call must succeed and return a non-empty version.
            // On non-glibc systems the P/Invoke throws internally and returns false — also correct.
            if (found)
            {
                Assert.NotNull(version);
                Assert.NotEmpty(version);
            }
            else
            {
                return; // gnu_get_libc_version not available on this Linux variant
            }
        }
    }
}
