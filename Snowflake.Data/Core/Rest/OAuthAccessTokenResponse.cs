using System;
using Newtonsoft.Json;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Rest
{
    internal class OAuthAccessTokenResponse
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

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<OAuthAccessTokenResponse>();
        private const int MaxRecommendedAccessTokenExpirationInSeconds = 60 * 60 * 24; // 1 day
        private const int MaxRecommendedRefreshTokenExpirationInSeconds = 60 * 60 * 24 * 30; // 1 month

        public void Validate()
        {
            if (string.IsNullOrEmpty(AccessToken))
            {
                s_logger.Error("Access token was not returned by Identity Provider");
                throw new Exception("Expected access token");
            }
            ValidateExpirationTime(ExpiresIn, "access token", MaxRecommendedAccessTokenExpirationInSeconds);
            if (!string.IsNullOrEmpty(RefreshToken))
            {
                ValidateExpirationTime(RefreshTokenExpiresIn, "refresh token", MaxRecommendedRefreshTokenExpirationInSeconds);
            }
        }

        private void ValidateExpirationTime(string expiresIn, string tokenName, int maxExpiresIn)
        {
            if (string.IsNullOrEmpty(expiresIn) || !(int.TryParse(expiresIn, out var expiresInIntValue) && expiresInIntValue > 0))
            {
                s_logger.Warn($"OAuth {tokenName} returned by IDP has expiration time which is not a positive integer value");
                return;
            }
            if (expiresInIntValue > maxExpiresIn)
            {
                s_logger.Warn($"OAuth {tokenName} returned by IDP will expire after: {expiresInIntValue} seconds which is a very long time");
            }
        }
    }
}
