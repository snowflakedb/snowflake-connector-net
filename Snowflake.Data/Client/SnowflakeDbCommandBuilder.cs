using System.Data;
using System.Data.Common;
using System.Globalization;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbCommandBuilder : DbCommandBuilder
    {
        public const string DEFAULT_QUOTE_PREFIX = "\"";
        public const string DEFAULT_QUOTE_SUFFIX = "\"";

        /// <summary>
        /// Initializes a new instance of the <see cref="SnowflakeDbCommandBuilder"/> class.
        /// </summary>
        public SnowflakeDbCommandBuilder()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SnowflakeDbCommandBuilder"/> class.
        /// </summary>
        /// <param name="adapter">The adapter.</param>
        public SnowflakeDbCommandBuilder(SnowflakeDbDataAdapter adapter)
        {
            DataAdapter = adapter;
            QuotePrefix = DEFAULT_QUOTE_PREFIX;
            QuoteSuffix = DEFAULT_QUOTE_SUFFIX;
        }

        /// <summary>
        /// Gets or sets the beginning character or characters to use when specifying database objects (for example, tables or columns) whose names contain characters such as spaces or reserved tokens.
        /// </summary>
        /// <returns>
        /// The beginning character or characters to use. The default is an empty string.
        ///   </returns>
        ///   <PermissionSet>
        ///   <IPermission class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" version="1" PathDiscovery="*AllFiles*" />
        ///   </PermissionSet>
        public sealed override string QuotePrefix
        {
            get => base.QuotePrefix;
            set => base.QuotePrefix = string.IsNullOrEmpty(value) ? DEFAULT_QUOTE_PREFIX : value;
        }

        /// <summary>
        /// Gets or sets the ending character or characters to use when specifying database objects (for example, tables or columns) whose names contain characters such as spaces or reserved tokens.
        /// </summary>
        /// <returns>
        /// The ending character or characters to use. The default is an empty string.
        ///   </returns>
        ///   <PermissionSet>
        ///   <IPermission class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089" version="1" PathDiscovery="*AllFiles*" />
        ///   </PermissionSet>
        public sealed override string QuoteSuffix
        {
            get => base.QuoteSuffix;
            set => base.QuoteSuffix = string.IsNullOrEmpty(value) ? DEFAULT_QUOTE_SUFFIX : value;
        }

        /// <summary>
        /// Applies the parameter information.
        /// </summary>
        /// <param name="p">The parameter.</param>
        /// <param name="row">The row.</param>
        /// <param name="statementType">Type of the statement.</param>
        /// <param name="whereClause">if set to <c>true</c> [where clause].</param>
        protected override void ApplyParameterInfo(DbParameter p, DataRow row, StatementType statementType, bool whereClause)
        {
            var param = (SnowflakeDbParameter)p;
            param.DbType = (DbType)row[SchemaTableColumn.ProviderType];
        }

        /// <summary>
        /// Returns the name of the specified parameter in the format of #.
        /// </summary>
        /// <param name="parameterOrdinal">The number to be included as part of the parameter's name..</param>
        /// <returns>
        /// The name of the parameter with the specified number appended as part of the parameter name.
        /// </returns>
        protected override string GetParameterName(int parameterOrdinal)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}", parameterOrdinal);
        }

        /// <summary>
        /// Returns the full parameter name, given the partial parameter name.
        /// </summary>
        /// <param name="parameterName">The partial name of the parameter.</param>
        /// <returns>
        /// The full parameter name corresponding to the partial parameter name requested.
        /// </returns>
        protected override string GetParameterName(string parameterName)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}", parameterName);
        }

        /// <summary>
        /// Returns the placeholder for the parameter in the associated SQL statement.
        /// </summary>
        /// <param name="parameterOrdinal">The number to be included as part of the parameter's name.</param>
        /// <returns>
        /// The name of the parameter with the specified number appended.
        /// </returns>
        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return GetParameterName(parameterOrdinal);
        }

        /// <inheritdoc />
        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
        }
    }
}
