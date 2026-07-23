namespace Snowflake.Data.Core.CredentialManager
{
    internal enum TokenType
    {
        [StringAttr(value = "ID_TOKEN")]
        IdToken,
        [StringAttr(value = "MFA_TOKEN")]
        MFAToken,
        [StringAttr(value = "OAUTH_ACCESS_TOKEN")]
        OAuthAccessToken,
        [StringAttr(value = "OAUTH_REFRESH_TOKEN")]
        OAuthRefreshToken
    }

    internal static class TokenTypeExtensions
    {
        /// <summary>
        /// Returns the PascalCase token-type string used as the third segment of the
        /// cache key prefix (<c>SnowflakeTokenCache.v2.&lt;TokenType&gt;.&lt;hash&gt;</c>).
        /// This is distinct from <c>GetAttribute&lt;StringAttr&gt;().value</c>, which returns
        /// the REST-protocol wire value and must not be used to build cache keys.
        /// </summary>
        internal static string ToCacheKeyPrefix(this TokenType tokenType) => tokenType switch
        {
            TokenType.IdToken          => "IdToken",
            TokenType.MFAToken         => "MfaToken",
            TokenType.OAuthAccessToken => "OauthAccessToken",
            TokenType.OAuthRefreshToken => "OauthRefreshToken",
            _ => throw new System.ArgumentOutOfRangeException(nameof(tokenType), tokenType, "Unknown TokenType")
        };
    }
}
