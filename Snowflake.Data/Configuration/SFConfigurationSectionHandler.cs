/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Collections;
using System.Configuration;
using System.Xml;

namespace Snowflake.Data.Configuration
{
    /// <summary>
    ///     Develop In progress
    /// </summary>
    public class SFConfigurationSectionHandler : IConfigurationSectionHandler
    {
        static SFConfigurationSectionHandler()
        { }

        public SFConfigurationSectionHandler()
        { }

        #region IConfigurationSectionHandler Memebers
        object IConfigurationSectionHandler.Create(object parent, object configContext, XmlNode section)
        {
            return null;
        }
        #endregion
    }

}
