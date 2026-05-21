using System;
using System.Collections.Generic;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{

    class SFSessionTimezoneTest
    {
        [SFFact]
        public void TestGetSessionTimezoneReturnsLocalWhenFeatureDisabled()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=false",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "Asia/Tokyo";

            Assert.Equal(TimeZoneInfo.Local, session.GetSessionTimezone());
        }

        [SFFact]
        public void TestGetSessionTimezoneFallsBackWhenTimezoneNotInParameterMap()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            Assert.Equal(TimeZoneInfo.Local, session.GetSessionTimezone());
        }

        [SFFact]
        public void TestGetSessionTimezoneFallsBackOnUnknownTimezone()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "Invalid/Nowhere";

            Assert.Equal(TimeZoneInfo.Local, session.GetSessionTimezone());
        }

        [SFFact]
        public void TestGetSessionTimezoneCachesResult()
        {
            var session = new SFSession(
                "account=test;user=test;password=test;HonorSessionTimezone=true",
                new SessionPropertiesContext());

            session.ParameterMap[SFSessionParameter.TIMEZONE] = "America/New_York";

            var first = session.GetSessionTimezone();
            var second = session.GetSessionTimezone();

            Assert.Same(first, second);
        }

        [SFFact]
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

            Assert.NotEqual(beforeUpdate, afterUpdate);
            Assert.Equal(expectedTokyo, afterUpdate);
        }

        [SFFact]
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
            Assert.Equal(expected, result);
        }
    }
}
