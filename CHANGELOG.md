#### For the official .NET Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/dotnet

# Changelog
- v5.0.0
    - Added support for alternative, memory efficient and thread safe CRL (Certificate Revocation List) checks.
    - Enforced TLS 1.3 on supported environments.
    - Removed `log4net` dependency and enabled delegated logging.
    - Upgraded AWS SDK library to v4.
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Removed internal classes from public API.

