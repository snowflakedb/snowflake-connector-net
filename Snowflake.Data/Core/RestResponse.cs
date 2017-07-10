using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Snowflake.Data.Core
{
    public abstract class BaseRestResponse
    {
        [JsonProperty(PropertyName = "message")]
        public String message { get; set; }
        
        [JsonProperty(PropertyName = "code")]
        public String code { get; set; }
        
        [JsonProperty(PropertyName = "success")]
        public bool success { get; set; }
    }

    public class DeleteSessionResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        public string data { get; set; }
    }

    public class AuthnResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        public AuthnResponseData data { get; set; }
    }

    public class AuthnResponseData
    {
        [JsonProperty(PropertyName = "token", NullValueHandling = NullValueHandling.Ignore)]
        public string token { get; set; }

        [JsonProperty(PropertyName = "validityInSeconds", NullValueHandling = NullValueHandling.Ignore)]
        public Int16 validityInSeconds { get; set; }

        [JsonProperty(PropertyName = "masterToken", NullValueHandling = NullValueHandling.Ignore)]
        public string masterToken { get; set; }

        [JsonProperty(PropertyName = "masterValidityInSeconds", NullValueHandling = NullValueHandling.Ignore)]
        public Int16 masterTokenValidityInSeconds { get; set; }

        [JsonProperty(PropertyName = "displayUserName", NullValueHandling = NullValueHandling.Ignore)]
        public string displayUserName { get; set; }

        [JsonProperty(PropertyName = "serverVersion", NullValueHandling = NullValueHandling.Ignore)]
        public string serverVersion { get; set; }

        [JsonProperty(PropertyName = "firstLogin", NullValueHandling = NullValueHandling.Ignore)]
        public bool firstLogin { get; set; }

        [JsonProperty(PropertyName = "healthCheckInterval", NullValueHandling = NullValueHandling.Ignore)]
        public Int16 healthCheckInterval { get; set; }

        [JsonProperty(PropertyName = "newClientForUpgrade", NullValueHandling = NullValueHandling.Ignore)]
        public string newClientForUpgrade { get; set; }

        [JsonProperty(PropertyName = "sessionId", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 sessionId { get; set; }

        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        public List<NameValueParameter> nameValueParameter { get; set; }

        [JsonProperty(PropertyName = "sessionInfo", NullValueHandling = NullValueHandling.Ignore)]
        public SessionInfo authResponseSessionInfo { get; set; }

        [JsonProperty(PropertyName = "tokenUrl", NullValueHandling = NullValueHandling.Ignore)]
        public string tokenUrl { get; set; }

        [JsonProperty(PropertyName = "ssoUrl", NullValueHandling = NullValueHandling.Ignore)]
        public string ssoUrl { get; set; }
    }

    public class SessionInfo
    {
        [JsonProperty(PropertyName = "databaseName")]
        public string databaseName { get; set; }

        [JsonProperty(PropertyName = "schemaName")]
        public string schemaName { get; set; }

        [JsonProperty(PropertyName = "warehouseName")]
        public string warehouseName { get; set; }

        [JsonProperty(PropertyName = "roleName")]
        public string roleName { get; set; }
    }

    public class NameValueParameter
    {
        [JsonProperty(PropertyName = "name")]
        public string name { get; set; }

        [JsonProperty(PropertyName = "value")]
        public string value { get; set; }
    }

    public class QueryExecResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        public QueryExecResponseData data { get; set; }
    }

    public class QueryExecResponseData
    {
        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        public List<NameValueParameter> parameters { get; set; }

        [JsonProperty(PropertyName = "rowtype", NullValueHandling = NullValueHandling.Ignore)]
        public List<ExecResponseRowType> rowType { get; set; }

        [JsonProperty(PropertyName = "rowset", NullValueHandling = NullValueHandling.Ignore)]
        public string[,] rowSet { get; set; }

        [JsonProperty(PropertyName = "total", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 total { get; set; }

        [JsonProperty(PropertyName = "returned", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 returned { get; set; }

        [JsonProperty(PropertyName = "queryId", NullValueHandling = NullValueHandling.Ignore)]
        public string queryId { get; set; }

        [JsonProperty(PropertyName = "sqlState", NullValueHandling = NullValueHandling.Ignore)]
        public string sqlState { get; set; }

        [JsonProperty(PropertyName = "databaseProvider", NullValueHandling = NullValueHandling.Ignore)]
        public string databaseProvider { get; set; }

        [JsonProperty(PropertyName = "finalDatabaseName", NullValueHandling = NullValueHandling.Ignore)]
        public string finalDatabaseName { get; set; }

        [JsonProperty(PropertyName = "finalSchemaName", NullValueHandling = NullValueHandling.Ignore)]
        public string finalSchemaName { get; set; }

        [JsonProperty(PropertyName = "finalWarehouseName", NullValueHandling = NullValueHandling.Ignore)]
        public string finalWarehouseName { get; set; }

        [JsonProperty(PropertyName = "finalRoleName", NullValueHandling = NullValueHandling.Ignore)]
        public string finalRoleName { get; set; }

        [JsonProperty(PropertyName = "numberOfBinds", NullValueHandling = NullValueHandling.Ignore)]
        public int numberOfBinds { get; set; }
        
        [JsonProperty(PropertyName = "statementTypeId", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 statementTypeId { get; set; }

        [JsonProperty(PropertyName = "version", NullValueHandling = NullValueHandling.Ignore)]
        public int version { get; set; }

        [JsonProperty(PropertyName = "chunks", NullValueHandling = NullValueHandling.Ignore)]
        public List<ExecResponseChunk> chunks { get; set; }

        [JsonProperty(PropertyName = "qrmk", NullValueHandling = NullValueHandling.Ignore)]
        public string qrmk { get; set; }

        // ping pong response data
        [JsonProperty(PropertyName = "getResultUrl", NullValueHandling = NullValueHandling.Ignore)]
        public string getResultUrl { get; set; }

        [JsonProperty(PropertyName = "progressDesc", NullValueHandling = NullValueHandling.Ignore)]
        public string progressDesc { get; set; }

        [JsonProperty(PropertyName = "queryAbortAfterSecs", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 queryAbortAfterSecs { get; set; }
    }

    public class ExecResponseRowType
    {
        [JsonProperty(PropertyName = "name")]
        public string name { get; set; }

        [JsonProperty(PropertyName = "byteLength", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 byteLength { get; set; }

        [JsonProperty(PropertyName = "length", NullValueHandling = NullValueHandling.Ignore)]
        public Int64 length { get; set; }

        [JsonProperty(PropertyName = "type")]
        public string type { get; set; }

        [JsonProperty(PropertyName = "scale")]
        public Int64 scale { get; set; }

        [JsonProperty(PropertyName = "precision")]
        public Int64 precision { get; set; }

        [JsonProperty(PropertyName = "nullable")]
        public bool nullable { get; set; }
    }
    
    public class ExecResponseChunk
    {
        [JsonProperty(PropertyName = "url")]
        public string url { get; set; }

        [JsonProperty(PropertyName = "rowCount")]
        public int rowCount { get; set; }
    }
 } 