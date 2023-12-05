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
using System.Text.RegularExpressions;

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
        [SFSessionPropertyAttr(required = false, defaultValue = "300")]
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
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        CLIENT_SESSION_KEEP_ALIVE,
        [SFSessionPropertyAttr(required = false)]
        GCS_USE_DOWNSCOPED_CREDENTIAL,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        FORCEPARSEERROR,
        [SFSessionPropertyAttr(required = false, defaultValue = "120")]
        BROWSER_RESPONSE_TIMEOUT,
        [SFSessionPropertyAttr(required = false, defaultValue = "300")]
        RETRY_TIMEOUT,
        [SFSessionPropertyAttr(required = false, defaultValue = "7")]
        MAXHTTPRETRIES,
        [SFSessionPropertyAttr(required = false)]
        FILE_TRANSFER_MEMORY_THRESHOLD,
        [SFSessionPropertyAttr(required = false, defaultValue = "true")]
        INCLUDERETRYREASON,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        DISABLEQUERYCONTEXTCACHE,
        [SFSessionPropertyAttr(required = false)]
        CLIENT_CONFIG_FILE
    }

    class SFSessionPropertyAttr : Attribute
    {
        public bool required { get; set; }

        public string defaultValue { get; set; }
    }

    class SFSessionProperties : Dictionary<SFSessionProperty, String>
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFSessionProperties>();

        // Connection string properties to obfuscate in the log
        private static List<SFSessionProperty> secretProps =
            new List<SFSessionProperty>{
                SFSessionProperty.PASSWORD,
                SFSessionProperty.PRIVATE_KEY,
                SFSessionProperty.TOKEN,
                SFSessionProperty.PRIVATE_KEY_PWD,
                SFSessionProperty.PROXYPASSWORD,
            };
        
        private const string AccountRegexString = "^\\w[\\w.-]+\\w$";

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
                logger.Warn("Invalid connectionString", e);
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

            //handle DbConnectionStringBuilder missing cases
            string[] propertyEntry = connectionString.Split(';');
            foreach(string keyVal in propertyEntry)
            {
                if(keyVal.Length > 0)
                {
                    string[] tokens = keyVal.Split(new string[] { "=" }, StringSplitOptions.None);
                    if(tokens[0].ToUpper() == "DB" || tokens[0].ToUpper() == "SCHEMA" ||
                        tokens[0].ToLower() == "WAREHOUSE" || tokens[0].ToUpper() == "ROLE")
                    {
                        if (tokens.Length == 2)
                        {
                            SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                                typeof(SFSessionProperty), tokens[0].ToUpper());
                            properties[p]= tokens[1];
                        }
                    }
                    if(tokens[0].ToUpper() == "USER" || tokens[0].ToUpper() == "PASSWORD")
                    {
                        SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                                typeof(SFSessionProperty), tokens[0].ToUpper());
                        if (!properties.ContainsKey(p))
                        {
                            properties.Add(p, "");
                        }
                    }
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
            ValidateFileTransferMaxBytesInMemoryProperty(properties);
            ValidateAccountDomain(properties);
            
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

        private static void ValidateAccountDomain(SFSessionProperties properties)
        {
            var account = properties[SFSessionProperty.ACCOUNT];
            if (string.IsNullOrEmpty(account))
                return;
            var match = Regex.Match(account, AccountRegexString, RegexOptions.IgnoreCase);
            if (match.Success)
                return;
            logger.Error($"Invalid account {account}");
            throw new SnowflakeDbException(
                new Exception("Invalid account"),
                SFError.INVALID_CONNECTION_PARAMETER_VALUE,
                account,
                SFSessionProperty.ACCOUNT);
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

        private static void ValidateFileTransferMaxBytesInMemoryProperty(SFSessionProperties properties)
        {
            if (!properties.TryGetValue(SFSessionProperty.FILE_TRANSFER_MEMORY_THRESHOLD, out var maxBytesInMemoryString))
            {
                return;
            }

            var propertyName = SFSessionProperty.FILE_TRANSFER_MEMORY_THRESHOLD.ToString();
            int maxBytesInMemory;
            try
            {
                maxBytesInMemory = int.Parse(maxBytesInMemoryString);
            }
            catch (Exception e)
            {
                logger.Error($"Value for parameter {propertyName} could not be parsed");
                throw new SnowflakeDbException(e, SFError.INVALID_CONNECTION_PARAMETER_VALUE, maxBytesInMemoryString, propertyName);
            }
            
            if (maxBytesInMemory <= 0)
            {
                logger.Error($"Value for parameter {propertyName} should be greater than 0");
                throw new SnowflakeDbException(
                    new Exception($"Value for parameter {propertyName} should be greater than 0"),
                    SFError.INVALID_CONNECTION_PARAMETER_VALUE, maxBytesInMemoryString, propertyName);
            }
        }
        
        private static bool IsRequired(SFSessionProperty sessionProperty, SFSessionProperties properties)
        {
            if (sessionProperty.Equals(SFSessionProperty.PASSWORD))
            {
                var authenticatorDefined =
                    properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator);

                var authenticatorsWithoutPassword = new List<string>()
                {
                    ExternalBrowserAuthenticator.AUTH_NAME,
                    KeyPairAuthenticator.AUTH_NAME,
                    OAuthAuthenticator.AUTH_NAME
                };
                // External browser, jwt and oauth don't require a password for authenticating
                return !authenticatorDefined || !authenticatorsWithoutPassword
                    .Any(auth => auth.Equals(authenticator, StringComparison.OrdinalIgnoreCase));
            }
            else if (sessionProperty.Equals(SFSessionProperty.USER))
            {
                var authenticatorDefined =
                   properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator);

                var authenticatorsWithoutUsername = new List<string>()
                {
                    OAuthAuthenticator.AUTH_NAME,
                    ExternalBrowserAuthenticator.AUTH_NAME
                };
                return !authenticatorDefined || !authenticatorsWithoutUsername
                    .Any(auth => auth.Equals(authenticator, StringComparison.OrdinalIgnoreCase));
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
