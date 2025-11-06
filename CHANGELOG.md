#### For the official .NET Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/dotnet

# Changelog
- v5.1.0
    - Added `APPLICATION_PATH` to `CLIENT_ENVIRONMENT` sent during authentication to identify the application connecting to Snowflake.
    - Renew idle sessions in the pool if keep alive is enabled.
    - Added `CRLDOWNLOADMAXSIZE` connection parameter to limit the maximum size of CRL files downloaded during certificate revocation checks.
    - AWS WIF will now also check the application config and AWS profile credential store when determining the current AWS region
    - Allow users to configure the maximum amount of connections via `SERVICE_POINT_CONNECTION_LIMIT` property.
    - Add retry for HTTP 307/308 status codes
- v5.0.0
    - Disabled CRL checks by default.
    - Added support for alternative, memory efficient and thread safe CRL (Certificate Revocation List) checks.
    - Support for TLS 1.3 with new parameters MINTLS and MAXTLS controlling protocol version negotiated with the server.
    - Removed `log4net` dependency and enabled delegated logging.
    - Upgraded AWS SDK library to v4.
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Removed internal classes from public API.
    - Added support for explicitly setting Azure managed identity client ID via `MANAGED_IDENTITY_CLIENT_ID` environmen

