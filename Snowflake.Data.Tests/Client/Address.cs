using System.Collections.Generic;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class Address
    {
        public string city { get; set; }
        public string state { get; set; }
        public Zip zip { get; set; }

        public Address()
        {
        }

        public Address(string city, string state, Zip zip)
        {
            this.city = city;
            this.state = state;
            this.zip = zip;
        }
    }

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
    }

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

    public class Grades
    {
        public string[] Names { get; set; }

        public Grades()
        {
        }

        public Grades(string[] names)
        {
            Names = names;
        }
    }

    public class GradesWithList
    {
        public List<string> Names { get; set; }

        public GradesWithList()
        {
        }

        public GradesWithList(List<string> names)
        {
            Names = names;
        }
    }
}
