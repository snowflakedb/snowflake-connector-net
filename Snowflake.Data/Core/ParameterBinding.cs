using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    public class BindingDTO
    {
        string type { get; set;  }

        Object value { get; set; }
    }

    public class ParameterBindings : Dictionary<string, BindingDTO>
    {
    }
}
