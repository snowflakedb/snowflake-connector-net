using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.PackageTests
{
    [TestFixture]
    public sealed class LibcDetectorIT
    {
        [Test]
        [Platform(Exclude = "Linux")]
        public void TestDetectReturnsNotApplicableOnNonLinux()
        {
            // Act
            var (family, version) = LibcDetector.Instance.Detect();

            // Assert
            Assert.That(family, Is.EqualTo(LibcFamily.NotApplicable));
            Assert.That(version, Is.Null);
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestDetectReturnsKnownFamilyOnLinux()
        {
            // Act
            var (family, _) = LibcDetector.Instance.Detect();

            // Assert — on any Linux box we should always resolve to one of the known families
            Assert.That(family, Is.Not.EqualTo(LibcFamily.NotApplicable));
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestDetectReturnsVersionStringOnLinuxGlibc()
        {
            // Act
            var (family, version) = LibcDetector.Instance.Detect();

            if (family == LibcFamily.Glibc)
            {
                Assert.That(version, Is.Not.Null);
                Assert.That(version, Is.Not.Empty);
            }
            else
            {
                Assert.Pass($"System uses {family}; glibc version assertion not applicable");
            }
        }

        [Test]
        public void TestTryGetGlibcVersionDoesNotThrow()
        {
            // Act — must not throw on any platform
            Assert.DoesNotThrow(() => LibcDetector.TryGetGlibcVersion(out _));
        }

        [Test]
        [Platform(Exclude = "Linux")]
        public void TestTryGetGlibcVersionReturnsFalseOnNonLinux()
        {
            // Act
            var found = LibcDetector.TryGetGlibcVersion(out var version);

            // Assert
            Assert.That(found, Is.False);
            Assert.That(version, Is.Null);
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestTryGetGlibcVersionReturnsTrueOnLinuxGlibc()
        {
            // Act
            var found = LibcDetector.TryGetGlibcVersion(out var version);

            // On glibc systems the call must succeed and return a non-empty version.
            // On non-glibc systems the P/Invoke throws internally and returns false — also correct.
            if (found)
            {
                Assert.That(version, Is.Not.Null);
                Assert.That(version, Is.Not.Empty);
            }
            else
            {
                Assert.Pass("gnu_get_libc_version not available on this Linux variant");
            }
        }
    }
}
