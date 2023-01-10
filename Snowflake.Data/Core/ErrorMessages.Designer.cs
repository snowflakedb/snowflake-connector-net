﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Snowflake.Data.Core {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "4.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class ErrorMessages {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal ErrorMessages() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("Snowflake.Data.Core.ErrorMessages", typeof(ErrorMessages).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Column index {0} is out of bound of valid index..
        /// </summary>
        internal static string COLUMN_INDEX_OUT_OF_BOUND {
            get {
                return ResourceManager.GetString("COLUMN_INDEX_OUT_OF_BOUND", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Data reader has already been closed..
        /// </summary>
        internal static string DATA_READER_ALREADY_CLOSED {
            get {
                return ResourceManager.GetString("DATA_READER_ALREADY_CLOSED", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Snowflake Internal Error: {0}.
        /// </summary>
        internal static string INTERNAL_ERROR {
            get {
                return ResourceManager.GetString("INTERNAL_ERROR", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Connection string is invalid: {0}.
        /// </summary>
        internal static string INVALID_CONNECTION_STRING {
            get {
                return ResourceManager.GetString("INVALID_CONNECTION_STRING", resourceCulture);
            }
        }

        /// <summary>
        ///   Looks up a localized string similar to Invalid parameter value {0} for {1}.
        /// </summary>
        internal static string INVALID_CONNECTION_PARAMETER_VALUE
        {
            get
            {
                return ResourceManager.GetString("INVALID_CONNECTION_PARAMETER_VALUE", resourceCulture);
            }
        }

        /// <summary>
        ///   Looks up a localized string similar to Failed to convert data {0} from type {0} to type {1}..
        /// </summary>
        internal static string INVALID_DATA_CONVERSION {
            get {
                return ResourceManager.GetString("INVALID_DATA_CONVERSION", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Required property {0} is not provided..
        /// </summary>
        internal static string MISSING_CONNECTION_PROPERTY {
            get {
                return ResourceManager.GetString("MISSING_CONNECTION_PROPERTY", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Query has been cancelled..
        /// </summary>
        internal static string QUERY_CANCELLED {
            get {
                return ResourceManager.GetString("QUERY_CANCELLED", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Request reach its timeout..
        /// </summary>
        internal static string REQUEST_TIMEOUT {
            get {
                return ResourceManager.GetString("REQUEST_TIMEOUT", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Another query is already running against this statement..
        /// </summary>
        internal static string STATEMENT_ALREADY_RUNNING_QUERY {
            get {
                return ResourceManager.GetString("STATEMENT_ALREADY_RUNNING_QUERY", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Feature is not supported. .
        /// </summary>
        internal static string UNSUPPORTED_FEATURE {
            get {
                return ResourceManager.GetString("UNSUPPORTED_FEATURE", resourceCulture);
            }
        }

        /// <summary>
        ///   Looks up a localized string similar to "Could not read RSA private key {0} : {1}.".
        /// </summary>
        internal static string JWT_ERROR_READING_PK
        {
            get
            {
                return ResourceManager.GetString("JWT_ERROR_READING_PK", resourceCulture);
            }
        }

        /// <summary>
        ///   Looks up a localized string.
        /// </summary>
        internal static string UNSUPPORTED_DOTNET_TYPE
        {
            get
            {
                return ResourceManager.GetString("UNSUPPORTED_DOTNET_TYPE", resourceCulture);
            }
        }

        /// <summary>
        ///   Looks up a localized string.
        /// </summary>
        internal static string UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM
        {
            get
            {
                return ResourceManager.GetString("UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM", resourceCulture);
            }
        }

        /// <summary>
        ///   Looks up a localized string.
        /// </summary>
        internal static string MISSING_BIND_PARAMETERS
        {
            get
            {
                return ResourceManager.GetString("MISSING_BIND_PARAMETERS", resourceCulture);
            }
        }
    }
}
