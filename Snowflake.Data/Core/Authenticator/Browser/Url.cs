namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class Url
    {
        public string Value { get; }

        public string ValueWithoutSecrets { get; }

        public Url(string url) : this(url, url)
        {
        }

        public Url(string url, string maskedUrl)
        {
            Value = url;
            ValueWithoutSecrets = maskedUrl;
        }
    }
}
