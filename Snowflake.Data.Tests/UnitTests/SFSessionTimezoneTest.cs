using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFSessionTimezoneTest
    {
        [Test]
        public void TestGetSessionTimezoneReturnsLocalWhenFeatureDisabled()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=false",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "Asia/Tokyo";

            Assert.AreEqual(TimeZoneInfo.Local, session.GetSessionTimezone());
        }

        [Test]
        public void TestGetSessionTimezoneFallsBackWhenTimezoneNotInParameterMap()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            Assert.AreEqual(TimeZoneInfo.Local, session.GetSessionTimezone());
        }

        [Test]
        public void TestGetSessionTimezoneFallsBackOnUnknownTimezone()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "Invalid/Nowhere";

            Assert.AreEqual(TimeZoneInfo.Local, session.GetSessionTimezone());
        }

        [Test]
        public void TestGetSessionTimezoneCachesResult()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "America/New_York";

            var first = session.GetSessionTimezone();
            var second = session.GetSessionTimezone();

            Assert.AreSame(first, second);
        }

        [Test]
        public void TestGetSessionTimezoneCacheInvalidatedOnParameterUpdate()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "America/New_York";
            var beforeUpdate = session.GetSessionTimezone();

            session.UpdateSessionParameterMap(new List<NameValueParameter>
            {
                new NameValueParameter { name = "TIMEZONE", value = "Asia/Tokyo" }
            });

            var afterUpdate = session.GetSessionTimezone();
            var expectedTokyo = TimeZoneConverter.TZConvert.GetTimeZoneInfo("Asia/Tokyo");

            Assert.AreNotEqual(beforeUpdate, afterUpdate);
            Assert.AreEqual(expectedTokyo, afterUpdate);
        }

        [Test]
        [TestCase("America/Los_Angeles")]
        [TestCase("UTC")]
        [TestCase("Asia/Tokyo")]
        [TestCase("Europe/Warsaw")]
        [TestCase("Pacific/Honolulu")]
        public void TestGetSessionTimezoneResolvesVariousTimezones(string tzName)
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = tzName;

            var result = session.GetSessionTimezone();
            var expected = TimeZoneConverter.TZConvert.GetTimeZoneInfo(tzName);
            Assert.AreEqual(expected, result);
        }
    }
}
