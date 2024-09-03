namespace Snowflake.Data.Tests.Client
{
    public class Identity
    {
        public string Name { get; set; }

        public Identity()
        {
        }

        public Identity(string name)
        {
            Name = name;
        }

        protected bool Equals(Identity other)
        {
            return Name == other.Name;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Identity)obj);
        }

        public override int GetHashCode()
        {
            return (Name != null ? Name.GetHashCode() : 0);
        }
    }
}
