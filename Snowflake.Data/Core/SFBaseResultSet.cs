using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    abstract class SFBaseResultSet
    {

        public abstract bool next();

        public string getString(int columnIndex)
        {
            return (String)getObjectInternal(columnIndex);
        }

        protected abstract Object getObjectInternal(int columnIndex);

    }
}
