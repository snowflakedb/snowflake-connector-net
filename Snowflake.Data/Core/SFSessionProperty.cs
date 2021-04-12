/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Security;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;

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
        [SFSessionPropertyAttr(required = true, defaultValue = "")] 
        USER,
        [SFSessionPropertyAttr(required = false)]
        WAREHOUSE,
        [SFSessionPropertyAttr(required = false, defaultValue = "0")]
        CONNECTION_TIMEOUT,
        [SFSessionPropertyAttr(required = false, defaultValue = "snowflake")]
        AUTHENTICATOR,
        [SFSessionPropertyAttr(required = false, defaultValue = "true")]
        VALIDATE_DEFAULT_PARAMETERS,
        [SFSessionPropertyAttr(required = false)]
        PRIVATE_KEY_FILE,
        [SFSessionPropertyAttr(required = false)]
        PRIVATE_KEY_PWD,
        [SFSessionPropertyAttr(required = false)]
        PRIVATE_KEY,
        [SFSessionPropertyAttr(required = false)]
        TOKEN,
    }

    class SFSessionPropertyAttr : Attribute
    {
        public bool required { get; set; }

        public string defaultValue { get; set; }
    }

    class SFSessionProperties : Dictionary<SFSessionProperty, String>
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFSessionProperties>();

        // Connection string properties to obfuscate in the log
        static private List<SFSessionProperty> secretProps = 
            new List<SFSessionProperty>{
                SFSessionProperty.PASSWORD,
                SFSessionProperty.PRIVATE_KEY,
                SFSessionProperty.TOKEN,
                SFSessionProperty.PRIVATE_KEY_PWD};

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

        public override int GetHashCode()
        {
            return base.GetHashCode();
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
                    string[] tokens = keyVal.Split(new string[] { "=" }, StringSplitOptions.None);
                    if (tokens.Length != 2)
                    {
                        // https://docs.microsoft.com/en-us/dotnet/api/system.data.oledb.oledbconnection.connectionstring
                        // To include an equal sign (=) in a keyword or value, it must be preceded 
                        // by another equal sign. For example, in the hypothetical connection 
                        // string "key==word=value" :
                        // the keyword is "key=word" and the value is "value".
                        int currentIndex = 0;
                        int singleEqualIndex = -1;
                        while (currentIndex <= keyVal.Length)
                        {
                            currentIndex = keyVal.IndexOf("=", currentIndex);
                            if (-1 == currentIndex)
                            {
                                // No '=' found
                                break;
                            }
                            if ((currentIndex < (keyVal.Length - 1)) && 
                                ('=' != keyVal[currentIndex + 1]))
                            {
                                if (0 > singleEqualIndex)
                                {
                                    // First single '=' encountered
                                    singleEqualIndex = currentIndex;
                                    currentIndex++;
                                }
                                else
                                {
                                    // Found another single '=' which is not allowed
                                    singleEqualIndex = -1;
                                    break;
                                }
                            }
                            else
                            {
                                // skip the doubled one
                                currentIndex += 2;
                            }
                        }
                        
                        if ((singleEqualIndex > 0) && (singleEqualIndex < keyVal.Length - 1))
                        {
                            // Split the key/value at the right index and deduplicate '=='
                            tokens = new string[2];
                            tokens[0] = keyVal.Substring(0, singleEqualIndex).Replace("==","=");
                            tokens[1] = keyVal.Substring(
                                singleEqualIndex + 1, 
                                keyVal.Length - (singleEqualIndex + 1)).Replace("==", "="); ;
                        }
                        else
                        {
                            // An equal sign was not doubled or something else happended
                            // making the connection invalid
                            string invalidStringDetail =
                                String.Format("Invalid key value pair {0}", keyVal);
                            SnowflakeDbException e =
                                new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING,
                                new object[] { invalidStringDetail });
                            logger.Error("Invalid string.", e);
                            throw e;
                        }
                    }

                    try
                    {
                        SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                            typeof(SFSessionProperty), tokens[0].ToUpper());
                        properties.Add(p, tokens[1]);
                        logger.Info($"Connection property: {p}, value: {(secretProps.Contains(p) ? "XXXXXXXX" : tokens[1])}");
                    }
                    catch (ArgumentException e)
                    {
                        logger.Warn($"Property {tokens[0]} not found ignored.", e);
                    }
                }
            }

            if (password != null)
            {
                properties[SFSessionProperty.PASSWORD] = new NetworkCredential(string.Empty, password).Password;
            }
            checkSessionProperties(properties);

            // compose host value if not specified
            if (!properties.ContainsKey(SFSessionProperty.HOST) || 
                (0 == properties[SFSessionProperty.HOST].Length))
            {
                string hostName = String.Format("{0}.snowflakecomputing.com", properties[SFSessionProperty.ACCOUNT]);
                // Remove in case it's here but empty
                properties.Remove(SFSessionProperty.HOST);
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
                var authenticatorDefined =
                    properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator);

                // External browser, jwt and oauth don't require a password for authenticating
                return !(authenticatorDefined &&
                        (authenticator.Equals(ExternalBrowserAuthenticator.AUTH_NAME,
                            StringComparison.OrdinalIgnoreCase) ||
                        authenticator.Equals(KeyPairAuthenticator.AUTH_NAME,
                            StringComparison.OrdinalIgnoreCase) ||
                        authenticator.Equals(OAuthAuthenticator.AUTH_NAME,
                        StringComparison.OrdinalIgnoreCase)));
            }
            else if (sessionProperty.Equals(SFSessionProperty.USER))
            {
                var authenticatorDefined =
                   properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator);

                // Oauth don't require a username for authenticating
                return !(authenticatorDefined && (
                    authenticator.Equals(OAuthAuthenticator.AUTH_NAME, StringComparison.OrdinalIgnoreCase)));
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
