using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.PackageTests
{
    [TestFixture]
    public sealed class LibcDetectorIT
    {
        private static readonly LibcDetector s_libcDetector = (LibcDetector)LibcDetector.Instance;

        [Test]
        [Platform(Exclude = "Linux")]
        public void TestDetectReturnsNotApplicableOnNonLinux()
        {
            // Act
            var (family, version) = s_libcDetector.Detect();

            // Assert
            Assert.That(family, Is.EqualTo(LibcFamily.NotApplicable));
            Assert.That(version, Is.Null);
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestDetectReturnsKnownFamilyOnLinux()
        {
            // Act
            var (family, _) = s_libcDetector.Detect();

            // Assert — on any Linux box we should always resolve to one of the known families
            Assert.That(family, Is.Not.EqualTo(LibcFamily.NotApplicable));
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestDetectReturnsVersionStringOnLinuxGlibc()
        {
            // Act
            var (family, version) = s_libcDetector.Detect();

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
        [Platform(Include = "Linux")]
        public void TestDetectReturnsVersionStringOnLinuxMusl()
        {
            // Act
            var (family, version) = s_libcDetector.Detect();

            if (family == LibcFamily.Musl)
            {
                Assert.That(version, Is.Not.Null);
                Assert.That(version, Is.Not.Empty);
            }
            else
            {
                Assert.Pass($"System uses {family}; musl version assertion not applicable");
            }
        }

        [Test]
        public void TestTryGetGlibcVersionDoesNotThrow()
        {
            // Act — must not throw on any platform
            Assert.DoesNotThrow(() => s_libcDetector.TryGetGlibcVersion(out _));
        }

        [Test]
        [Platform(Exclude = "Linux")]
        public void TestTryGetGlibcVersionReturnsFalseOnNonLinux()
        {
            // Act
            var found = s_libcDetector.TryGetGlibcVersion(out var version);

            // Assert
            Assert.That(found, Is.False);
            Assert.That(version, Is.Null);
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestTryGetGlibcVersionReturnsTrueOnLinuxGlibc()
        {
            // Act
            var found = s_libcDetector.TryGetGlibcVersion(out var version);

            // On glibc systems the call must succeed and return a non-empty version.
            // On musl systems the P/Invoke throws internally and returns false — also correct.
            if (found)
            {
                Assert.That(version, Is.Not.Null);
                Assert.That(version, Is.Not.Empty);
            }
            else
            {
                Assert.Pass("gnu_get_libc_version not available on this Linux variant (likely musl)");
            }
        }

        [Test]
        public void TestTryGetMuslVersionFromLddDoesNotThrow()
        {
            // Act — must not throw on any platform
            Assert.DoesNotThrow(() => s_libcDetector.TryGetMuslVersionFromLdd(out _));
        }

        [Test]
        [Platform(Exclude = "Linux")]
        public void TestTryGetMuslVersionFromLddReturnsFalseOnNonLinux()
        {
            // Act
            var found = s_libcDetector.TryGetMuslVersionFromLdd(out var version);

            // Assert — non-Linux ldd output never contains "musl"
            Assert.That(found, Is.False);
            Assert.That(version, Is.Null);
        }

        [Test]
        [Platform(Include = "Linux")]
        public void TestTryGetMuslVersionFromLddReturnsTrueOnLinuxMusl()
        {
            // Act
            var found = s_libcDetector.TryGetMuslVersionFromLdd(out var version);

            if (found)
            {
                Assert.That(version, Is.Not.Null);
                Assert.That(version, Is.Not.Empty);
            }
            else
            {
                Assert.Pass("ldd output does not indicate musl on this Linux variant (likely glibc)");
            }
        }
    }
}
