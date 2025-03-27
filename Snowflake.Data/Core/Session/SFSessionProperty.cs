using System;
using System.Collections.Generic;
using System.Net;
using System.Security;
using Snowflake.Data.Log;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Snowflake.Data.Core.Tools;
using System.Runtime.InteropServices;

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
        [SFSessionPropertyAttr(required = true, IsSecret = true)]
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
        [SFSessionPropertyAttr(required = false, IsSecret = true)]
        PRIVATE_KEY_PWD,
        [SFSessionPropertyAttr(required = false, IsSecret = true)]
        PRIVATE_KEY,
        [SFSessionPropertyAttr(required = false, IsSecret = true)]
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
        [SFSessionPropertyAttr(required = false, IsSecret = true)]
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
        CLIENT_CONFIG_FILE,
        [SFSessionPropertyAttr(required = false, defaultValue = "true")]
        DISABLE_CONSOLE_LOGIN,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        ALLOWUNDERSCORESINHOST,
        [SFSessionPropertyAttr(required = false)]
        QUERY_TAG,
        [SFSessionPropertyAttr(required = false, defaultValue = "10")]
        MAXPOOLSIZE,
        [SFSessionPropertyAttr(required = false, defaultValue = "2")]
        MINPOOLSIZE,
        [SFSessionPropertyAttr(required = false, defaultValue = "Destroy")]
        CHANGEDSESSION,
        [SFSessionPropertyAttr(required = false, defaultValue = "30s")]
        WAITINGFORIDLESESSIONTIMEOUT,
        [SFSessionPropertyAttr(required = false, defaultValue = "60m")]
        EXPIRATIONTIMEOUT,
        [SFSessionPropertyAttr(required = false, defaultValue = "true")]
        POOLINGENABLED,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        DISABLE_SAML_URL_CHECK,
        [SFSessionPropertyAttr(required = false, defaultValue = "true", defaultNonWindowsValue = "false")]
        CLIENT_STORE_TEMPORARY_CREDENTIAL,
        [SFSessionPropertyAttr(required = false, IsSecret = true)]
        PASSCODE,
        [SFSessionPropertyAttr(required = false, defaultValue = "false")]
        PASSCODEINPASSWORD
    }

    class SFSessionPropertyAttr : Attribute
    {
        public bool required { get; set; }

        public string defaultValue { get; set; }

        public string defaultNonWindowsValue { get; set; }

        public bool IsSecret { get; set; } = false;
    }

    class SFSessionProperties : Dictionary<SFSessionProperty, String>
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFSessionProperties>();

        internal string ConnectionStringWithoutSecrets { get; set; }

        internal bool IsPoolingEnabledValueProvided { get; set; }

        // Connection string properties to obfuscate in the log
        private static readonly List<string> s_secretProps = Enum.GetValues(typeof(SFSessionProperty))
            .Cast<SFSessionProperty>()
            .Where(p => p.GetAttribute<SFSessionPropertyAttr>().IsSecret)
            .Select(p => p.ToString())
            .ToList();

        private static readonly List<string> s_accountRegexStrings = new List<string>
        {
            "^\\w",
            "\\w$",
            "^[\\w.-]+$"
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

        internal static SFSessionProperties ParseConnectionString(string connectionString, SecureString password, SecureString passcode = null)
        {
            logger.Info("Start parsing connection string.");
            var builder = new DbConnectionStringBuilder();
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
            var properties = new SFSessionProperties();

            var keys = new string[builder.Keys.Count];
            var values = new string[builder.Values.Count];
            builder.Keys.CopyTo(keys, 0);
            builder.Values.CopyTo(values,0);

            properties.ConnectionStringWithoutSecrets = BuildConnectionStringWithoutSecrets(ref keys, ref values);

            for(var i=0; i<keys.Length; i++)
            {
                try
                {
                    SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                                typeof(SFSessionProperty), keys[i].ToUpper());
                    properties.Add(p, values[i]);
                }
                catch (ArgumentException)
                {
                    logger.Debug($"Property {keys[i]} not found ignored.");
                }
            }

            UpdatePropertiesForSpecialCases(properties, connectionString);

            var useProxy = false;
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

            if (password != null && password.Length > 0)
            {
                properties[SFSessionProperty.PASSWORD] = SecureStringHelper.Decode(password);
            }

            if (passcode != null && passcode.Length > 0)
            {
                properties[SFSessionProperty.PASSCODE] = SecureStringHelper.Decode(passcode);
            }

            ValidateAuthenticator(properties);
            ValidatePasscodeInPassword(properties);
            ValidateClientStoreTemporaryCredential(properties);
            properties.IsPoolingEnabledValueProvided = properties.IsNonEmptyValueProvided(SFSessionProperty.POOLINGENABLED);
            CheckSessionProperties(properties);
            ValidateFileTransferMaxBytesInMemoryProperty(properties);
            ValidateAccountDomain(properties);

            var allowUnderscoresInHost = ParseAllowUnderscoresInHost(properties);

            // compose host value if not specified
            if (!properties.ContainsKey(SFSessionProperty.HOST) ||
                (0 == properties[SFSessionProperty.HOST].Length))
            {
                var compliantAccountName = properties[SFSessionProperty.ACCOUNT];
                if (!allowUnderscoresInHost && compliantAccountName.Contains('_'))
                {
                    compliantAccountName = compliantAccountName.Replace('_', '-');
                    logger.Info($"Replacing _ with - in the account name. Old: {properties[SFSessionProperty.ACCOUNT]}, new: {compliantAccountName}.");
                }
                var hostName = $"{compliantAccountName}.snowflakecomputing.com";
                // Remove in case it's here but empty
                properties.Remove(SFSessionProperty.HOST);
                properties.Add(SFSessionProperty.HOST, hostName);
                logger.Info($"Compose host name: {hostName}");
            }
            logger.Info(ResolveConnectionAreaMessage(properties[SFSessionProperty.HOST]));

            // Trim the account name to remove the region and cloud platform if any were provided
            // because the login request data does not expect region and cloud information to be
            // passed on for account_name
            properties[SFSessionProperty.ACCOUNT] = properties[SFSessionProperty.ACCOUNT].Split('.')[0];

            return properties;
        }

        internal static string ResolveConnectionAreaMessage(string host) =>
            host.EndsWith(".cn", StringComparison.InvariantCultureIgnoreCase)
                ? "Connecting to CHINA Snowflake domain"
                : "Connecting to GLOBAL Snowflake domain";

        private static void ValidateAuthenticator(SFSessionProperties properties)
        {
            var knownAuthenticators = new Func<string, bool>[]
            {
                BasicAuthenticator.IsBasicAuthenticator,
                OktaAuthenticator.IsOktaAuthenticator,
                OAuthAuthenticator.IsOAuthAuthenticator,
                KeyPairAuthenticator.IsKeyPairAuthenticator,
                ExternalBrowserAuthenticator.IsExternalBrowserAuthenticator,
                MFACacheAuthenticator.IsMfaCacheAuthenticator
            };

            if (properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator))
            {
                if (!knownAuthenticators.Any(func => func(authenticator)))
                {
                    authenticator = authenticator.ToLower();
                    var error = $"Unknown authenticator: {authenticator}";
                    logger.Error(error);
                    throw new SnowflakeDbException(SFError.UNKNOWN_AUTHENTICATOR, authenticator);
                }
            }
        }

        private static void ValidatePasscodeInPassword(SFSessionProperties properties)
        {
            if (properties.TryGetValue(SFSessionProperty.PASSCODEINPASSWORD, out var passCodeInPassword))
            {
                if (!bool.TryParse(passCodeInPassword, out _))
                {
                    var errorMessage = $"Invalid value of {SFSessionProperty.PASSCODEINPASSWORD.ToString()} parameter";
                    logger.Error(errorMessage);
                    throw new SnowflakeDbException(
                        new Exception(errorMessage),
                        SFError.INVALID_CONNECTION_PARAMETER_VALUE,
                        "",
                        SFSessionProperty.PASSCODEINPASSWORD.ToString());
                }
            }
        }

        private static void ValidateClientStoreTemporaryCredential(SFSessionProperties properties)
        {
            if (properties.TryGetValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, out var clientStoreTemporaryCredential))
            {
                if (!bool.TryParse(clientStoreTemporaryCredential, out _))
                {
                    var errorMessage = $"Invalid value of {SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL} parameter";
                    logger.Error(errorMessage);
                    throw new SnowflakeDbException(
                        new Exception(errorMessage),
                        SFError.INVALID_CONNECTION_PARAMETER_VALUE,
                        "",
                        SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL.ToString());
                }
            }
        }

        internal bool IsNonEmptyValueProvided(SFSessionProperty property) =>
            TryGetValue(property, out var propertyValueStr) && !string.IsNullOrEmpty(propertyValueStr);

        private static string BuildConnectionStringWithoutSecrets(ref string[] keys, ref string[] values)
        {
            var count = keys.Length;
            var result = new StringBuilder();
            for (var i = 0; i < count; i++ )
            {
                if (!IsSecretProperty(keys[i]))
                {
                    result.Append(keys[i]);
                    result.Append("=");
                    result.Append(values[i]);
                    result.Append(";");
                }
            }
            return result.ToString();
        }

        private static bool IsSecretProperty(string propertyName)
        {
            return s_secretProps.Contains(propertyName, StringComparer.OrdinalIgnoreCase);
        }

        private static void UpdatePropertiesForSpecialCases(SFSessionProperties properties, string connectionString)
        {
            var propertyEntry = connectionString.Split(';');
            foreach(var keyVal in propertyEntry)
            {
                if(keyVal.Length > 0)
                {
                    var tokens = keyVal.Split(new string[] { "=" }, StringSplitOptions.None);
                    var propertyName = tokens[0].ToUpper();
                    switch (propertyName)
                    {
                        case "DB":
                        case "SCHEMA":
                        case "WAREHOUSE":
                        case "ROLE":
                        {
                            if (tokens.Length == 2)
                            {
                                var sessionProperty = (SFSessionProperty)Enum.Parse(
                                    typeof(SFSessionProperty), propertyName);
                                properties[sessionProperty]= ProcessObjectEscapedCharacters(tokens[1]);
                            }

                            break;
                        }
                        case "USER":
                        case "PASSWORD":
                        {

                            var sessionProperty = (SFSessionProperty)Enum.Parse(
                                typeof(SFSessionProperty), propertyName);
                            if (!properties.ContainsKey(sessionProperty))
                            {
                                properties.Add(sessionProperty, "");
                            }

                            break;
                        }
                    }
                }
            }
        }

        private static string ProcessObjectEscapedCharacters(string objectValue)
        {
            var match = Regex.Match(objectValue, "^\"(.*)\"$");
            if(match.Success)
            {
                var replaceEscapedQuotes = match.Groups[1].Value.Replace("\"\"", "\"");
                return $"\"{replaceEscapedQuotes}\"";
            }

            return objectValue;
        }

        private static void ValidateAccountDomain(SFSessionProperties properties)
        {
            var account = properties[SFSessionProperty.ACCOUNT];
            if (string.IsNullOrEmpty(account))
                return;
            if (IsAccountRegexMatched(account))
                return;
            logger.Error($"Invalid account {account}");
            throw new SnowflakeDbException(
                new Exception("Invalid account"),
                SFError.INVALID_CONNECTION_PARAMETER_VALUE,
                account,
                SFSessionProperty.ACCOUNT);
        }

        private static bool IsAccountRegexMatched(string account) =>
            s_accountRegexStrings
                .Select(regex => Regex.Match(account, regex, RegexOptions.IgnoreCase))
                .All(match => match.Success);

        private static void CheckSessionProperties(SFSessionProperties properties)
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            foreach (SFSessionProperty sessionProperty in Enum.GetValues(typeof(SFSessionProperty)))
            {
                // if required property, check if exists in the dictionary
                if (IsRequired(sessionProperty, properties) &&
                    !properties.ContainsKey(sessionProperty))
                {
                    SnowflakeDbException e = new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY, sessionProperty);
                    logger.Error("Missing connection property", e);
                    throw e;
                }

                if (IsRequired(sessionProperty, properties) && string.IsNullOrEmpty(properties[sessionProperty]))
                {
                    SnowflakeDbException e = new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY, sessionProperty);
                    logger.Error("Empty connection property", e);
                    throw e;
                }

                // add default value to the map
                string defaultVal = sessionProperty.GetAttribute<SFSessionPropertyAttr>().defaultValue;
                string defaultNonWindowsVal = sessionProperty.GetAttribute<SFSessionPropertyAttr>().defaultNonWindowsValue;
                if (!properties.ContainsKey(sessionProperty))
                {
                    if (defaultNonWindowsVal != null && !isWindows)
                    {
                        logger.Debug($"Session property {sessionProperty} set to default value: {defaultNonWindowsVal}");
                        properties.Add(sessionProperty, defaultNonWindowsVal);
                    }
                    else if (defaultVal != null)
                    {
                        logger.Debug($"Session property {sessionProperty} set to default value: {defaultVal}");
                        properties.Add(sessionProperty, defaultVal);
                    }
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

                var authenticatorsWithoutPassword = new Func<string, bool>[]
                {
                    ExternalBrowserAuthenticator.IsExternalBrowserAuthenticator,
                    KeyPairAuthenticator.IsKeyPairAuthenticator,
                    OAuthAuthenticator.IsOAuthAuthenticator
                };
                // External browser, jwt and oauth don't require a password for authenticating
                return !authenticatorDefined || !authenticatorsWithoutPassword.Any(func => func(authenticator));
            }
            else if (sessionProperty.Equals(SFSessionProperty.USER))
            {
                var authenticatorDefined =
                   properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator);

                var authenticatorsWithoutUsername = new Func<string, bool>[]
                {
                    OAuthAuthenticator.IsOAuthAuthenticator,
                    ExternalBrowserAuthenticator.IsExternalBrowserAuthenticator
                };
                return !authenticatorDefined || !authenticatorsWithoutUsername.Any(func => func(authenticator));
            }
            else if (sessionProperty.Equals(SFSessionProperty.TOKEN))
            {
                var authenticatorDefined = properties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator);

                return !authenticatorDefined || OAuthAuthenticator.IsOAuthAuthenticator(authenticator);
            }
            else
            {
                return sessionProperty.GetAttribute<SFSessionPropertyAttr>().required;
            }
        }

        private static bool ParseAllowUnderscoresInHost(SFSessionProperties properties)
        {
            var allowUnderscoresInHost = bool.Parse(SFSessionProperty.ALLOWUNDERSCORESINHOST.GetAttribute<SFSessionPropertyAttr>().defaultValue);
            if (!properties.TryGetValue(SFSessionProperty.ALLOWUNDERSCORESINHOST, out var property))
                return allowUnderscoresInHost;
            try
            {
                allowUnderscoresInHost = bool.Parse(property);
            }
            catch (Exception e)
            {
                logger.Warn("Unable to parse property 'allowUnderscoresInHost'", e);
            }

            return allowUnderscoresInHost;
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
