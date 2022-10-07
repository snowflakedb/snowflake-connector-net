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
using System.Data.Common;
using System.Linq;

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
        [SFSessionPropertyAttr(required = false, defaultValue = "120")]
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
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        INSECUREMODE,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        USEPROXY,
        [SFSessionPropertyAttr(required = false)]
        PROXYHOST,
        [SFSessionPropertyAttr(required = false)]
        PROXYPORT,
        [SFSessionPropertyAttr(required = false)]
        PROXYUSER,
        [SFSessionPropertyAttr(required = false)]
        PROXYPASSWORD,
        [SFSessionPropertyAttr(required = false)]
        NONPROXYHOSTS,
        [SFSessionPropertyAttr(required = false)]
        APPLICATION,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        DISABLERETRY,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        FORCERETRYON404,
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
                SFSessionProperty.PRIVATE_KEY_PWD,
                SFSessionProperty.PROXYPASSWORD,
            };

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
            DbConnectionStringBuilder builder = new DbConnectionStringBuilder();
            try
            { 
                builder.ConnectionString = connectionString;
            }
            catch (ArgumentException e)
            {
                logger.Warn($"ConnectionString: {connectionString}", e);
                throw new SnowflakeDbException(e,
                                SFError.INVALID_CONNECTION_STRING,
                                e.Message);
            }
            SFSessionProperties properties = new SFSessionProperties();

            string[] keys = new string[builder.Keys.Count];
            string[] values = new string[builder.Values.Count];
            builder.Keys.CopyTo(keys, 0);
            builder.Values.CopyTo(values,0);

            for(int i=0; i<keys.Length; i++)
            {
                try
                {
                    SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                                typeof(SFSessionProperty), keys[i].ToUpper());
                    properties.Add(p, values[i]);
                }
                catch (ArgumentException e)
                {
                    logger.Warn($"Property {keys[i]} not found ignored.", e);
                }
            }

            bool useProxy = false;
            if (properties.ContainsKey(SFSessionProperty.USEPROXY))
            {
                try
                {
                    useProxy = Boolean.Parse(properties[SFSessionProperty.USEPROXY]);
                }
                catch (Exception e)
                {
                    // The useProxy setting is not a valid boolean value
                    logger.Error("Unable to connect", e);
                    throw new SnowflakeDbException(e,
                                SFError.INVALID_CONNECTION_STRING,
                                e.Message);
                }
            }

            // Based on which proxy settings have been provided, update the required settings list
            if (useProxy)
            {
                // If useProxy is true, then proxyhost and proxy port are mandatory
                SFSessionProperty.PROXYHOST.GetAttribute<SFSessionPropertyAttr>().required = true;
                SFSessionProperty.PROXYPORT.GetAttribute<SFSessionPropertyAttr>().required = true;

                // If a username is provided, then a password is required
                if (properties.ContainsKey(SFSessionProperty.PROXYUSER))
                {
                    SFSessionProperty.PROXYPASSWORD.GetAttribute<SFSessionPropertyAttr>().required = true;
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

            // Trim the account name to remove the region and cloud platform if any were provided
            // because the login request data does not expect region and cloud information to be 
            // passed on for account_name
            properties[SFSessionProperty.ACCOUNT] = properties[SFSessionProperty.ACCOUNT].Split('.')[0];


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
                    logger.Error("Missing connection property", e);
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
