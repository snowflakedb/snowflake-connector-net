using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    public class BindingDTO
    {
        public BindingDTO(string type, string value)
        {
            this.type = type;
            this.value = value;
        }

        public string type { get; set;  }

        public string value { get; set; }
    }
}
