using System.Collections.Generic;

namespace Snowflake.Data.Tests.Client
{
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

    public class GradesWithMap
    {
        public Dictionary<string, string> Names { get; set; }

        public GradesWithMap()
        {
        }

        public GradesWithMap(Dictionary<string, string> names)
        {
            Names = names;
        }
    }
}
