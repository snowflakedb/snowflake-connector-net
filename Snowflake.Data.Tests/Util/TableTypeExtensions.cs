using System;
using NUnit.Framework;

namespace Snowflake.Data.Tests.Util
{
    public enum SFTableType
    {
        Standard,
        Hybrid,
        Iceberg
    }

    static class TableTypeExtensions
    {
        internal static string TableDDLCreationPrefix(this SFTableType val) => val == SFTableType.Standard ? "" : val.ToString().ToUpper();

        internal static string TableDDLCreationFlags(this SFTableType val)
        {
            if (val != SFTableType.Iceberg)
                return "";
            var externalVolume = Environment.GetEnvironmentVariable("ICEBERG_EXTERNAL_VOLUME");
            var catalog = Environment.GetEnvironmentVariable("ICEBERG_CATALOG");
            var baseLocation = Environment.GetEnvironmentVariable("ICEBERG_BASE_LOCATION");
            Assert.IsNotNull(externalVolume, "env ICEBERG_EXTERNAL_VOLUME not set!");
            Assert.IsNotNull(catalog, "env ICEBERG_CATALOG not set!");
            Assert.IsNotNull(baseLocation, "env ICEBERG_BASE_LOCATION not set!");
            return $"EXTERNAL_VOLUME = '{externalVolume}' CATALOG = '{catalog}' BASE_LOCATION = '{baseLocation}'";
        }
    }
}
