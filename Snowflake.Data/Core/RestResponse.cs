/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Snowflake.Data.Core
{
    abstract class BaseRestResponse
    {
        [JsonProperty(PropertyName = "message")]
        internal String message { get; set; }
        
        [JsonProperty(PropertyName = "code", NullValueHandling = NullValueHandling.Ignore)]
        internal int code { get; set; }
        
        [JsonProperty(PropertyName = "success")]
        internal bool success { get; set; }
    }

    class NullDataResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal string data { get; set; }
    }

    internal class AuthnResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal AuthnResponseData data { get; set; }
    }
    
    internal class RenewSessionResponse : BaseRestResponse {
		[JsonProperty(PropertyName = "data")]
		internal RenewSessionResponseData data { get; set; }
	}

    internal class AuthnResponseData
    {
        [JsonProperty(PropertyName = "token", NullValueHandling = NullValueHandling.Ignore)]
        internal string token { get; set; }

        [JsonProperty(PropertyName = "validityInSeconds", NullValueHandling = NullValueHandling.Ignore)]
        internal Int16 validityInSeconds { get; set; }

        [JsonProperty(PropertyName = "masterToken", NullValueHandling = NullValueHandling.Ignore)]
        internal string masterToken { get; set; }

        [JsonProperty(PropertyName = "masterValidityInSeconds", NullValueHandling = NullValueHandling.Ignore)]
        internal Int16 masterTokenValidityInSeconds { get; set; }

        [JsonProperty(PropertyName = "displayUserName", NullValueHandling = NullValueHandling.Ignore)]
        internal string displayUserName { get; set; }

        [JsonProperty(PropertyName = "serverVersion", NullValueHandling = NullValueHandling.Ignore)]
        internal string serverVersion { get; set; }

        [JsonProperty(PropertyName = "firstLogin", NullValueHandling = NullValueHandling.Ignore)]
        internal bool firstLogin { get; set; }

        [JsonProperty(PropertyName = "healthCheckInterval", NullValueHandling = NullValueHandling.Ignore)]
        internal Int16 healthCheckInterval { get; set; }

        [JsonProperty(PropertyName = "newClientForUpgrade", NullValueHandling = NullValueHandling.Ignore)]
        internal string newClientForUpgrade { get; set; }

        [JsonProperty(PropertyName = "sessionId", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 sessionId { get; set; }

        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        internal List<NameValueParameter> nameValueParameter { get; set; }

        [JsonProperty(PropertyName = "sessionInfo", NullValueHandling = NullValueHandling.Ignore)]
        internal SessionInfo authResponseSessionInfo { get; set; }

        [JsonProperty(PropertyName = "tokenUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string tokenUrl { get; set; }

        [JsonProperty(PropertyName = "ssoUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string ssoUrl { get; set; }
    }


	internal class RenewSessionResponseData {

		[JsonProperty(PropertyName = "sessionToken", NullValueHandling = NullValueHandling.Ignore)]
		internal string sessionToken { get; set; }

		[JsonProperty(PropertyName = "validityInSecondsST", NullValueHandling = NullValueHandling.Ignore)]
		internal Int16 masterTokenValidityInSeconds { get; set; }

		[JsonProperty(PropertyName = "masterToken", NullValueHandling = NullValueHandling.Ignore)]
		internal string masterToken { get; set; }

		[JsonProperty(PropertyName = "validityInSecondsMT", NullValueHandling = NullValueHandling.Ignore)]
		internal Int16 validityInSeconds { get; set; }

		[JsonProperty(PropertyName = "sessionId", NullValueHandling = NullValueHandling.Ignore)]
		internal Int64 sessionId { get; set; }
	}

    internal class SessionInfo
    {
        [JsonProperty(PropertyName = "databaseName")]
        internal string databaseName { get; set; }

        [JsonProperty(PropertyName = "schemaName")]
        internal string schemaName { get; set; }

        [JsonProperty(PropertyName = "warehouseName")]
        internal string warehouseName { get; set; }

        [JsonProperty(PropertyName = "roleName")]
        internal string roleName { get; set; }
    }

    internal class NameValueParameter
    {
        [JsonProperty(PropertyName = "name")]
        internal string name { get; set; }

        [JsonProperty(PropertyName = "value")]
        internal string value { get; set; }
    }

    internal class QueryExecResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal QueryExecResponseData data { get; set; }
    }

    internal class QueryExecResponseData
    {
        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        internal List<NameValueParameter> parameters { get; set; }

        [JsonProperty(PropertyName = "rowtype", NullValueHandling = NullValueHandling.Ignore)]
        internal List<ExecResponseRowType> rowType { get; set; }

        [JsonProperty(PropertyName = "rowset", NullValueHandling = NullValueHandling.Ignore)]
        internal string[,] rowSet { get; set; }

        [JsonProperty(PropertyName = "total", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 total { get; set; }

        [JsonProperty(PropertyName = "returned", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 returned { get; set; }

        [JsonProperty(PropertyName = "queryId", NullValueHandling = NullValueHandling.Ignore)]
        internal string queryId { get; set; }

        [JsonProperty(PropertyName = "sqlState", NullValueHandling = NullValueHandling.Ignore)]
        internal string sqlState { get; set; }

        [JsonProperty(PropertyName = "databaseProvider", NullValueHandling = NullValueHandling.Ignore)]
        internal string databaseProvider { get; set; }

        [JsonProperty(PropertyName = "finalDatabaseName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalDatabaseName { get; set; }

        [JsonProperty(PropertyName = "finalSchemaName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalSchemaName { get; set; }

        [JsonProperty(PropertyName = "finalWarehouseName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalWarehouseName { get; set; }

        [JsonProperty(PropertyName = "finalRoleName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalRoleName { get; set; }

        [JsonProperty(PropertyName = "numberOfBinds", NullValueHandling = NullValueHandling.Ignore)]
        internal int numberOfBinds { get; set; }

        [JsonProperty(PropertyName = "statementTypeId", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 statementTypeId { get; set; }

        [JsonProperty(PropertyName = "version", NullValueHandling = NullValueHandling.Ignore)]
        internal int version { get; set; }

        [JsonProperty(PropertyName = "chunks", NullValueHandling = NullValueHandling.Ignore)]
        internal List<ExecResponseChunk> chunks { get; set; }

        [JsonProperty(PropertyName = "qrmk", NullValueHandling = NullValueHandling.Ignore)]
        internal string qrmk { get; set; }

        [JsonProperty(PropertyName = "chunkHeaders", NullValueHandling = NullValueHandling.Ignore)]
        internal Dictionary<string, string> chunkHeaders { get; set; }

        // ping pong response data
        [JsonProperty(PropertyName = "getResultUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string getResultUrl { get; set; }

        [JsonProperty(PropertyName = "progressDesc", NullValueHandling = NullValueHandling.Ignore)]
        internal string progressDesc { get; set; }

        [JsonProperty(PropertyName = "queryAbortAfterSecs", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 queryAbortAfterSecs { get; set; }
    }

    internal class ExecResponseRowType
    {
        [JsonProperty(PropertyName = "name")]
        internal string name { get; set; }

        [JsonProperty(PropertyName = "byteLength", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 byteLength { get; set; }

        [JsonProperty(PropertyName = "length", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 length { get; set; }

        [JsonProperty(PropertyName = "type")]
        internal string type { get; set; }

        [JsonProperty(PropertyName = "scale", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 scale { get; set; }

        [JsonProperty(PropertyName = "precision", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 precision { get; set; }

        [JsonProperty(PropertyName = "nullable")]
        internal bool nullable { get; set; }
    }
    
    internal class ExecResponseChunk
    {
        [JsonProperty(PropertyName = "url")]
        internal string url { get; set; }

        [JsonProperty(PropertyName = "rowCount")]
        internal int rowCount { get; set; }
    }
 } 
