/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using Common.Logging;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    internal enum SFSessionProperty
    {
        [SFSessionPropertyAttr(required = true)]
        ACCOUNT,
        [SFSessionPropertyAttr(required = false)]
        DB,
        [SFSessionPropertyAttr(required = false)] 
        HOST,
        [SFSessionPropertyAttr(required = true)] 
        PASSWORD,
        [SFSessionPropertyAttr(required = false, defaultValue = "443")] 
        PORT,
        [SFSessionPropertyAttr(required = false)]
        ROLE,
        [SFSessionPropertyAttr(required = false)]
        SCHEMA,
        [SFSessionPropertyAttr(required = false, defaultValue = "https")] 
        SCHEME,
        [SFSessionPropertyAttr(required = true)] 
        USER,
        [SFSessionPropertyAttr(required = false)]
        WAREHOUSE,
        [SFSessionPropertyAttr(required = false, defaultValue = "-1")]
        CONNECTION_TIMEOUT
    }

    class SFSessionPropertyAttr : Attribute
    {
        public bool required { get; set; }

        public string defaultValue { get; set; }
    }

    class SFSessionProperties : Dictionary<SFSessionProperty, String>
    {
        static private ILog logger = LogManager.GetLogger<SFSessionProperties>();

        internal static SFSessionProperties parseConnectionString(String connectionString)
        {
            logger.Info("Start parsing connection string.");
            SFSessionProperties properties = new SFSessionProperties();

            string[] propertyEntry = connectionString.Split(new char[] { ';' }, StringSplitOptions.None);

            foreach (string keyVal in propertyEntry)
            {
                if (keyVal.Length > 0)
                {
                    string[] token = keyVal.Split(new string[] { "=" }, StringSplitOptions.None);
                    if (token.Length == 2)
                    {
                        try
                        {
                            SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                                typeof(SFSessionProperty), token[0].ToUpper());
                            properties.Add(p, token[1]);
                            logger.InfoFormat("Connection property: {0}, value: {1}", p,
                                p == SFSessionProperty.PASSWORD ? "XXXXXXXX" : token[1]);
                        }
                        catch (ArgumentException e)
                        {
                            logger.WarnFormat("Property {0} not found ignored.", token[0]);
                        }
                    }
                    else
                    {
                        string invalidStringDetail = String.Format("Invalid kay value pair {0}", keyVal);
                        SnowflakeDbException e = new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, 
                            new object[] { invalidStringDetail });
                        logger.Error(e);
                        throw e;
                    }
                }
            }

            checkSessionProperties(properties);

            // compose host value if not specified
            if (!properties.ContainsKey(SFSessionProperty.HOST))
            {
                string hostName = String.Format("%s.snowflakecomputing.com", properties[SFSessionProperty.ACCOUNT]);
                properties.Add(SFSessionProperty.HOST, hostName);
                logger.InfoFormat("Compose host name: {0}", hostName);
            }

            return properties; 
        }

        private static void checkSessionProperties(SFSessionProperties properties)
        {
            foreach (SFSessionProperty sessionProperty in Enum.GetValues(typeof(SFSessionProperty)))
            {
                // if required property, check if exists in the dictionary
                if (sessionProperty.GetAttribute<SFSessionPropertyAttr>().required &&
                    !properties.ContainsKey(sessionProperty))
                {
                    SnowflakeDbException e = new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY, 
                        sessionProperty);
                    logger.Error(e);
                    throw e;
                }
                
                // add default value to the map
                string defaultVal = sessionProperty.GetAttribute<SFSessionPropertyAttr>().defaultValue;
                if (defaultVal != null && !properties.ContainsKey(sessionProperty))
                {
                    logger.DebugFormat("Sesssion property {0} set to default value: {1}", sessionProperty, defaultVal);
                    properties.Add(sessionProperty, defaultVal);
                }
            }
        }
    }

    public static class EnumExtensions
    {
        public static TAttribute GetAttribute<TAttribute>(this Enum value)
            where TAttribute : Attribute
        {
            var type = value.GetType();
            var memInfo = type.GetMember(value.ToString());
            var attributes = memInfo[0].GetCustomAttributes(typeof(TAttribute), false);
            return (attributes.Length > 0) ? (TAttribute)attributes[0] : null;
        }
    }
}
