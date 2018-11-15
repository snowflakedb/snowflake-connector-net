using System.Configuration;

namespace Snowflake.Data.Core
{
    /*class SFConfigurationSectionHandler : IConfigurationSectionHandler
    {
        static SFConfigurationSectionHandler()
        { }

        public SFConfigurationSectionHandler()
        { }

        object IConfigurationSectionHandler.Create(object parent, object configContext, System.Xml.XmlNode section)
        {
            return Create(parent as SFSetting, configContext, section);
        }

        private SFSetting 
    }

    class SFConfiguration : ConfigurationSection
    {
        public static SFConfiguration Config { get; } = ConfigurationManager.GetSection("Snowflake")
            as SFConfiguration;

        [ConfigurationProperty("useV1Parser"
            , DefaultValue = false
            , IsRequired = false)]
        public bool UseV1Parser
        {
            get { return (bool)this["useV1Parser"]; }
            set { this["useV1Parser"] = value; }
        }

        [ConfigurationProperty("useV2ChunkDownloader"
            , DefaultValue = false
            , IsRequired = false)]
        public bool UseV2ChunkDownloader
        {
            get { return (bool)this["useV2ChunkDownloader"]; }
            set { this["useV2ChunkDownloader"] = value; }
        }
    }*/
}
