using System;
using Mono.Unix;

namespace Snowflake.Data.Log
{
    internal class UnixFilePermissionsConverter
    {
        internal static int ConvertFileAccessPermissionsToInt(FileAccessPermissions permissions)
        {
            int userPermission = 0;
            int groupPermission = 0;
            int otherPermission = 0;

            foreach (FileAccessPermissions perm in Enum.GetValues(typeof(FileAccessPermissions)))
            {
                // Check if enum exists in the directory permission
                if ((permissions & perm) != 0)
                {
                    string permName = perm.ToString();
                    if (!permName.Contains("ReadWriteExecute"))
                    {
                        if (permName.Contains("User"))
                        {
                            userPermission += (int)perm;
                        }
                        else if (permName.Contains("Group"))
                        {
                            groupPermission += (int)perm;
                        }
                        else if (permName.Contains("Other"))
                        {
                            otherPermission += (int)perm;
                        }
                    }
                }
            }

            // Divide by 2^6
            userPermission /= 64;

            // Divide by 2^3
            groupPermission /= 8;

            return Convert.ToInt32(string.Format("{0}{1}{2}", userPermission, groupPermission, otherPermission));
        }
    }
}
