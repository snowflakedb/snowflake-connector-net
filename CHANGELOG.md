#### For the official .NET Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/dotnet

# Changelog
- v5.0.0
    - Disabled CRL checks by default.
    - Added support for alternative, memory efficient and thread safe CRL (Certificate Revocation List) checks.
    - Support for TLS 1.3 with new parameters MINTLS and MAXTLS controlling protocol version negotiated with the server.
    - Removed `log4net` dependency and enabled delegated logging.
    - Upgraded AWS SDK library to v4.
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Removed internal classes from public API.
    - Added support for explicitly setting Azure managed identity client ID via `MANAGED_IDENTITY_CLIENT_ID` environment variable.
