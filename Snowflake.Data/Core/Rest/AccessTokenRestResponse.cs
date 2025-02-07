using System;
using Newtonsoft.Json;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Rest
{
    internal class AccessTokenRestResponse
    {
        [JsonProperty(PropertyName = "token_type", NullValueHandling = NullValueHandling.Ignore)]
        public string TokenType { get; set; }

        [JsonProperty(PropertyName = "expires_in", NullValueHandling = NullValueHandling.Ignore)]
        public string ExpiresIn { get; set; }

        [JsonProperty(PropertyName = "access_token", NullValueHandling = NullValueHandling.Ignore)]
        public string AccessToken { get; set; }

        [JsonProperty(PropertyName = "refresh_token", NullValueHandling = NullValueHandling.Ignore)]
        public string RefreshToken { get; set; }

        [JsonProperty(PropertyName = "refresh_token_expires_in", NullValueHandling = NullValueHandling.Ignore)]
        public string RefreshTokenExpiresIn { get; set; }

        [JsonProperty(PropertyName = "scope", NullValueHandling = NullValueHandling.Ignore)]
        public string Scope { get; set; }

        public Token GetAccessToken(DateTime now)
        {
            var expiresIn = int.Parse(ExpiresIn);
            var expirationTime = now.AddSeconds(expiresIn);
            return new Token
            {
                Value = SecureStringHelper.Encode(AccessToken),
                UtcExpirationTime = expirationTime
            };
        }

        public Token GetRefreshToken(DateTime now)
        {
            if (string.IsNullOrEmpty(RefreshToken))
                return null;
            var expirationTime = string.IsNullOrEmpty(RefreshTokenExpiresIn) ? (DateTime?) null : now.AddSeconds(int.Parse(RefreshTokenExpiresIn));
            return new Token
            {
                Value = SecureStringHelper.Encode(RefreshToken),
                UtcExpirationTime = expirationTime
            };
        }

        public void Validate()
        {
            if (TokenType != "Bearer")
                throw new Exception("Expected Bearer token type");
            if (string.IsNullOrEmpty(AccessToken))
                throw new Exception("Expected access token");
            if (!IsPositiveInteger(ExpiresIn))
                throw new Exception("Expected expiration time of access token to be a positive integer");
            if (!string.IsNullOrEmpty(RefreshToken) && !string.IsNullOrEmpty(RefreshTokenExpiresIn) && !IsPositiveInteger(RefreshTokenExpiresIn))
                throw new Exception("Expected expiration time of refresh token to be a positive integer");
            if (string.IsNullOrEmpty(Scope))
                throw new Exception("Expected scope to be not empty");
        }

        private bool IsPositiveInteger(string value)
        {
            if (string.IsNullOrEmpty(value))
                return false;
            return int.TryParse(value, out var intValue) && intValue > 0;
        }
    }
}
