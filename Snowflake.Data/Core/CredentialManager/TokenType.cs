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
}
