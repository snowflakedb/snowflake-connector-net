using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    internal enum SFSessionProperty
    {
        [SFSessionPropertyAttr(required = true)]
        ACCOUNT,
        [SFSessionPropertyAttr(required = false)]
        DB,
        [SFSessionPropertyAttr(required = true)] 
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
        WAREHOUSE
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
            string[] properties = connectionString.Split(new string[] { ";" }, StringSplitOptions.None);

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
        }
    }
}
