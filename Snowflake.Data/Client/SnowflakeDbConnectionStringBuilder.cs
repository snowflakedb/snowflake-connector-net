/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.ComponentModel;
using System.Data.Common;
#if NET6_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif

namespace Snowflake.Data.Client
{
    public class SnowflakeDbConnectionStringBuilder : DbConnectionStringBuilder, INotifyPropertyChanged
    {
        public SnowflakeDbConnectionStringBuilder() { }

        public SnowflakeDbConnectionStringBuilder(string conn)
        {
            ConnectionString = conn;
        }

        #region INotifyPropertyChanged implementation

#if NET6_0_OR_GREATER
        public event PropertyChangedEventHandler? PropertyChanged;
#else
        public event PropertyChangedEventHandler PropertyChanged;
#endif

#if NET6_0_OR_GREATER
        [AllowNull]
#endif
        public override object this[string key]
        {
            get => base[key];
            set
            {
                TryGetValue(key, out var existing);
                if (existing == null)
                {
                    if (value == null)
                    {
                        return;
                    }
                }
                else if (existing.Equals(value))
                {
                    return;
                }
                base[key] = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(key));
            }
        }
        #endregion

        #region Helpers
#if NET6_0_OR_GREATER
        protected string? GetString(string key) => TryGetValue(key, out var value) ? (string)value : null;
#else
        protected string GetString(string key) => TryGetValue(key, out var value) ? (string)value : null;
#endif

        protected bool GetBool(string key) => TryGetValue(key, out var value) ? Convert.ToBoolean(value) : false;

        protected int GetInt(string key)
        {
            var value = GetString(key);
            if (value != null && int.TryParse(value, out int result))
            {
                return result;
            }
            return 0;
        }
        #endregion

        #region Properties
        [Category("Required")]
        [Description("Your full account name. Might include additional segments that identify the region and cloud platform where your account is hosted.")]
        public string Account
        {
            get => GetString(nameof(Account).ToLower());
            set => this[nameof(Account).ToLower()] = value;
        }

        [Category("Required")]
        [Description("Username.  If Authenticator is set to 'externalbrowser' or the URL for native SSO through Okta, set this to the login name for your identity provider (IdP).")]
        public string User
        {
            get => GetString(nameof(User).ToLower());
            set => this[nameof(User).ToLower()] = value;
        }

        [Category("Required")]
        [DefaultValue("snowflake")]
        [Description(@"The method of authentication. Currently supports the following values:
- snowflake (default): You must also set USER and PASSWORD.
- the URL for native SSO through Okta: You must also set USER and PASSWORD.
- externalbrowser: You must also set USER.
- snowflake_jwt: You must also set PRIVATE_KEY_FILE or PRIVATE_KEY.
- oauth: You must also set TOKEN.")]
        public string Authenticator
        {
            get => GetString(nameof(Authenticator).ToLower());
            set => this[nameof(Authenticator).ToLower()] = value;
        }

        [Category("Auth")]
        [Description("Required if AUTHENTICATOR is set to 'snowflake' (the default value) or the URL for native SSO through Okta. Ignored for all the other authentication types.")]
        public string Password
        {
            get => GetString(nameof(Password).ToLower());
            set => this[nameof(Password).ToLower()] = value;
        }

        [Category("Auth")]
        [Description("The path to the private key file to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt")]
        public string PrivateKeyFile
        {
            get => GetString("private_key_file");
            set => this["private_key_file"] = value;
        }

        [Category("Auth")]
        [Description("The private key to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt.")]
        public string PrivateKey
        {
            get => GetString("private_key");
            set => this["private_key"] = value;
        }

        [Category("Auth")]
        [Description("The passphrase to use for decrypting the private key, if the key is encrypted.")]
        public string PrivateKeyPwd
        {
            get => GetString("private_key_pwd");
            set => this["private_key_pwd"] = value;
        }

        [Category("Auth")]
        [Description("The OAuth token to use for OAuth authentication. Must be used in combination with AUTHENTICATOR=oauth.")]
        public string Token
        {
            get => GetString(nameof(Token).ToLower());
            set => this[nameof(Token).ToLower()] = value;
        }

        [Category("Connection")]
        [Description(@"Specifies the hostname for your account in the following format: <HOST>.snowflakecomputing.com.
If no value is specified, the driver uses<ACCOUNT>.snowflakecomputing.com.")]
        public string Host
        {
            get => GetString(nameof(Host).ToLower());
            set => this[nameof(Host).ToLower()] = value;
        }

        [Category("Connection")]
        public string DB
        {
            get => GetString(nameof(DB).ToLower());
            set => this[nameof(DB).ToLower()] = value;
        }

        [Category("Connection")]
        public string Role
        {
            get => GetString(nameof(Role).ToLower());
            set => this[nameof(Role).ToLower()] = value;
        }

        [Category("Connection")]
        public string Schema
        {
            get => GetString(nameof(Schema).ToLower());
            set => this[nameof(Schema).ToLower()] = value;
        }

        [Category("Connection")]
        public string Warehouse
        {
            get => GetString(nameof(Warehouse).ToLower());
            set => this[nameof(Warehouse).ToLower()] = value;
        }

        [Category("Connection")]
        [DefaultValue(true)]
        [Description("Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default to be true.")]
        public bool ValidateDefaultParameters
        {
            get => GetBool("validate_default_parameters");
            set => this["validate_default_parameters"] = value;
        }

        [Category("Connection")]
        [DefaultValue(false)]
        [Description("Set to true to disable the certificate revocation list check. Default is false.")]
        public bool InsecureMode
        {
            get => GetBool(nameof(InsecureMode).ToLower());
            set => this[nameof(InsecureMode).ToLower()] = value;
        }

        [Category("Connection")]
        [DefaultValue(120)]
        [Description("Total timeout in seconds when connecting to Snowflake. Default to 120 seconds.")]
        public int ConnectionTimeout
        {
            get => GetInt("connection_timeout");
            set => this["connection_timeout"] = value;
        }

        [Category("Connection")]
        [DefaultValue(false)]
        [Description("Set this property to true to prevent the driver from reconnecting automatically when the connection fails or drops. The default value is false.")]
        public bool DisableRetry
        {
            get => GetBool(nameof(DisableRetry).ToLower());
            set => this[nameof(DisableRetry).ToLower()] = value;
        }

        [Category("Proxy")]
        [DefaultValue(false)]
        [Description("Set to true if you need to use a proxy server. The default value is false.")]
        public bool UseProxy
        {
            get => GetBool(nameof(UseProxy).ToLower());
            set => this[nameof(UseProxy).ToLower()] = value;
        }

        [Category("Proxy")]
        [Description(@"The hostname of the proxy server.
If USEPROXY is set to true, you must set this parameter. ")]
        public string ProxyHost
        {
            get => GetString(nameof(ProxyHost).ToLower());
            set => this[nameof(ProxyHost).ToLower()] = value;
        }

        [Category("Proxy")]
        [Description(@"The hostname of the proxy server.
If USEPROXY is set to true, you must set this parameter.")]
        public int ProxyPort
        {
            get => GetInt(nameof(ProxyPort).ToLower());
            set => this[nameof(ProxyPort).ToLower()] = value;
        }

        [Category("Proxy")]
        [Description("The username for authenticating to the proxy server.")]
        public string ProxyUser
        {
            get => GetString(nameof(ProxyUser).ToLower());
            set => this[nameof(ProxyUser).ToLower()] = value;
        }

        [Category("Proxy")]
        [Description(@"The password for authenticating to the proxy server.
If USEPROXY is true and PROXYUSER is set, you must set this parameter.")]
        public string ProxyPassword
        {
            get => GetString(nameof(ProxyPassword).ToLower());
            set => this[nameof(ProxyPassword).ToLower()] = value;
        }

        [Category("Proxy")]
        [Description("The list of hosts that the driver should connect to directly, bypassing the proxy server. Separate the hostnames with a pipe symbol (|). You can also use an asterisk (*) as a wildcard.")]
        public string NonProxyHosts
        {
            get => GetString(nameof(NonProxyHosts).ToLower());
            set => this[nameof(NonProxyHosts).ToLower()] = value;
        }

        [Category("Partner")]
        [Description("Snowflake partner use only: Specifies the name of a partner application to connect through .NET. The name must match the following pattern: ^[A-Za-z]([A-Za-z0-9.-]){1,50}$ (one letter followed by 1 to 50 letter, digit, .,- or, _ characters).")]
        public string Application
        {
            get => GetString(nameof(Application).ToLower());
            set => this[nameof(Application).ToLower()] = value;
        }
        #endregion
    }
}
