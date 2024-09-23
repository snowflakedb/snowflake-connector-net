namespace Snowflake.Data.Tests.Client
{
    public class Zip
    {
        public string prefix { get; set; }
        public string postfix { get; set; }

        public Zip()
        {
        }

        public Zip(string prefix, string postfix)
        {
            this.prefix = prefix;
            this.postfix = postfix;
        }

        protected bool Equals(Zip other)
        {
            return prefix == other.prefix && postfix == other.postfix;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Zip)obj);
        }

        public override int GetHashCode()
        {
#if NETFRAMEWORK
            return prefix.GetHashCode() * 177 + postfix.GetHashCode();
#else
            return System.HashCode.Combine(prefix, postfix);
#endif
        }
    }
}
