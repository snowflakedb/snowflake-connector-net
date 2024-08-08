namespace Snowflake.Data.Tests.Client
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
}
