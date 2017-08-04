using System;
using System.Collections.Generic;

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
        internal SFSessionProperties(String connectionString)
        {
            string[] properties = connectionString.Split(new char[] { ';' }, StringSplitOptions.None);

            foreach (string keyVal in properties)
            {
                string[] token = keyVal.Split(new string[] { "=" }, StringSplitOptions.None);
                if (token.Length == 2)
                {
                    SFSessionProperty p = (SFSessionProperty)Enum.Parse(
                        typeof(SFSessionProperty), token[0].ToUpper());
                    this.Add(p, token[1]);
                }
            }

            checkSessionProperties();

            // compose host value if not specified
            if (!this.ContainsKey(SFSessionProperty.HOST))
            {
                this.Add(SFSessionProperty.HOST, String.Format("%s.snowflakecomputing.com", 
                    this[SFSessionProperty.ACCOUNT]));
            }
        }

        private void checkSessionProperties()
        {
            foreach (SFSessionProperty sessionProperty in Enum.GetValues(typeof(SFSessionProperty)))
            {
                // if required property, check if exists in the dictionary
                if (sessionProperty.GetAttribute<SFSessionPropertyAttr>().required &&
                    !this.ContainsKey(sessionProperty))
                {
                    throw new SFException(SFError.MISSING_CONNECTION_PROPERTY, 
                        sessionProperty);
                }
                
                // add default value to the map
                string defaultVal = sessionProperty.GetAttribute<SFSessionPropertyAttr>().defaultValue;
                if (defaultVal != null && !this.ContainsKey(sessionProperty))
                {
                    this.Add(sessionProperty, defaultVal);
                }
            }
        }

        /// <summary>
        ///     Check that given a value in string format if it it valid  
        /// </summary>
        /// <returns>true if the value of a property is valid</returns>
        private bool isValueValid(string value)
        {
            return true;
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
