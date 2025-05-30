using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    public class BindingDTO
    {
        public BindingDTO(string type, object value)
        {
            this.type = type;
            this.value = value;
        }

        public string type { get; set; }

        public object value { get; set; }
    }
}
