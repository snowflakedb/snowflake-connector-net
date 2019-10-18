/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Security;
using Snowflake.Data.Log;
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
        [SFSessionPropertyAttr(required = false, defaultValue = "0")]
        CONNECTION_TIMEOUT,
        [SFSessionPropertyAttr(required = false, defaultValue = "snowflake")]
        AUTHENTICATOR,
        [SFSessionPropertyAttr(required = false, defaultValue = "true")]
        VALIDATE_DEFAULT_PARAMETERS,
    }

    class SFSessionPropertyAttr : Attribute
    {
        public bool required { get; set; }

        public string defaultValue { get; set; }
    }

    class SFSessionProperties : Dictionary<SFSessionProperty, String>
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFSessionProperties>();

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            try
            {
                SFSessionProperties prop = (SFSessionProperties)obj;
                foreach (SFSessionProperty sessionProperty in Enum.GetValues(typeof(SFSessionProperty)))
                {
                    if (this.ContainsKey(sessionProperty) ^ prop.ContainsKey(sessionProperty))
                    {
                        return false;
                    }
                    if (!this.ContainsKey(sessionProperty))
                    {
                        continue;
                    }
                    if (!this[sessionProperty].Equals(prop[sessionProperty]))
                    {
                        return false;
                    }
                }
                return true;
            }
            catch (InvalidCastException)
            {
                logger.Warn("Invalid casting to SFSessionProperties");
                return false;
            }
        }

        internal static SFSessionProperties parseConnectionString(String connectionString, SecureString password)
        {
            logger.Info("Start parsing connection string.");
            SFSessionProperties properties = new SFSessionProperties();

            string[] propertyEntry = connectionString.Split(';');

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
                            logger.Info($"Connection property: {p}, value: {(p == SFSessionProperty.PASSWORD ? "XXXXXXXX" : token[1])}");
                        }
                        catch (ArgumentException e)
                        {
                            logger.Warn($"Property {token[0]} not found ignored.", e);
                        }
                    }
                    else
                    {
                        string invalidStringDetail = String.Format("Invalid key value pair {0}", keyVal);
                        SnowflakeDbException e = new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING,
                            new object[] { invalidStringDetail });
                        logger.Error("Invalid string.", e);
                        throw e;
                    }
                }
            }

            if (password != null)
            {
                properties[SFSessionProperty.PASSWORD] = new NetworkCredential(string.Empty, password).Password;
            }
            checkSessionProperties(properties);

            // compose host value if not specified
            if (!properties.ContainsKey(SFSessionProperty.HOST))
            {
                string hostName = String.Format("{0}.snowflakecomputing.com", properties[SFSessionProperty.ACCOUNT]);
                properties.Add(SFSessionProperty.HOST, hostName);
                logger.Info($"Compose host name: {hostName}");
            }

            return properties;
        }

        private static void checkSessionProperties(SFSessionProperties properties)
        {
            foreach (SFSessionProperty sessionProperty in Enum.GetValues(typeof(SFSessionProperty)))
            {
                // if required property, check if exists in the dictionary
                if (IsRequired(sessionProperty, properties) &&
                    !properties.ContainsKey(sessionProperty))
                {
                    SnowflakeDbException e = new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY,
                        sessionProperty);
                    logger.Error("Missing connetion property", e);
                    throw e;
                }

                // add default value to the map
                string defaultVal = sessionProperty.GetAttribute<SFSessionPropertyAttr>().defaultValue;
                if (defaultVal != null && !properties.ContainsKey(sessionProperty))
                {
                    logger.Debug($"Sesssion property {sessionProperty} set to default value: {defaultVal}");
                    properties.Add(sessionProperty, defaultVal);
                }
            }
        }

        private static bool IsRequired(SFSessionProperty sessionProperty, SFSessionProperties properties)
        {
            if (sessionProperty.Equals(SFSessionProperty.PASSWORD))
            {
                return !(properties.ContainsKey(SFSessionProperty.AUTHENTICATOR)
                    && properties[SFSessionProperty.AUTHENTICATOR] == "externalbrowser");
            }
            else
            {
                return sessionProperty.GetAttribute<SFSessionPropertyAttr>().required;
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
