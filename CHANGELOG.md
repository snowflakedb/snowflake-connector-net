#### For the official .NET Release Notes please refer to https://docs.snowflake.com/en/release-notes/clients-drivers/dotnet

# Changelog
- v5.8.0
  -  Replaced NUnit tests with Xunit in order to modernize and stabilize existing CI/CD setup.
<<<<<<< mcepiga/ktlo-crl
  -  Improved handling of certificates serial number matching and performance of CRL checkup.
=======
  -  Bug fix: Fixed incorrect DateTime conversion for timestamps preceding Unix epoch (1970-01-01) when fractional seconds are
    present.
>>>>>>> master
- v5.7.0
    - Improved input handling in `ChangeDatabase` by using parameterized queries.
    - Improved input validation in `QueryResultsAwaiter` with stricter UUID format checks.
    - Bug fix: `OverflowException` when converting rest response with master token validity more than ~9.1h.
    - Bug fix: Added path traversal protection for file downloads: destination paths are now validated against the target base directory before writing.
    - Bug fix: Replaced use of System.Random with a cryptographically secure random number generator in the authenticator challenge/proof key generation and file transfer encryption key/IV generation.
- v5.6.0
    - Added client-side telemetry instrumentation using `System.Diagnostics.Activity` (OpenTelemetry-compatible). When `CLIENT_TELEMETRY_ENABLED=true` (the default), the driver automatically instruments all command executions
      and their async variants and sends telemetry data to Snowflake's `/telemetry/send` endpoint. Activities are enriched with session context (warehouse, role, database, session id) and report success/error
      status with exception details.
    - Added public `StartActivity` extension method on `SnowflakeDbCommand` for creating custom client-defined
      telemetry activities. Custom activities use a separate activity source (`Client_custom_activity`).
    - Added .NET 10 support. Changed LangVersion to C#13.
    - Added `DbType.AnsiStringFixedLength` to the set of types mapped to Snowflake `TEXT`, matching existing support for `AnsiString`, `String`, and `StringFixedLength`.
    - Extended login-request telemetry with libc detection (`LIBC_FAMILY`, `LIBC_VERSION`). On Linux, the driver now reports whether the runtime uses glibc and includes the library version.
    - Limited default maximal CRL size for download to 20MB.
    - Bug fix: Fixed handling of transient server issues resulting in sending truncated JSON response.
    - Bug fix: Connections with sessions that no longer exist on the server are now detected and removed from the pool instead of being reused, which previously caused repeated failures until the connection expired on its own.
- v5.5.0
    - Include `SPCS_TOKEN` in login requests when running inside an SPCS container (`SNOWFLAKE_RUNNING_INSIDE_SPCS` env var set).
    - Extended login-request telemetry with cloud platform and environment detection (AWS Lambda, EC2, Azure VM/Functions, GCE/Cloud Run, GitHub Actions). Detection runs once at startup in the background within a 200ms timeout. Can be disabled via the `SNOWFLAKE_DISABLE_PLATFORM_DETECTION` environment variable.
    - Added `workloadIdentityImpersonationPath` config option for `authenticator=WORKLOAD_IDENTITY` allowing workloads to authenticate as a different identity through transitive service account impersonation.
    - Added `HonorSessionTimezone` connection parameter (default: `false`). When set to `true`, TIMESTAMP_LTZ values honor the session TIMEZONE parameter (`ALTER SESSION SET TIMEZONE`) instead of using the local machine timezone. This will become the default behavior in a future major release.
    - Bug fix: Idle sessions are now evicted from the pool even when closing them fails.
    - Bug fix: Sessions that receive HTTP 401 during query execution are no longer returned to the pool.
    - Bug fix: Fixed `GetResultsFromQueryIdAsync` not aborting queries on the server when `CancellationToken` is cancelled. Previously only client-side polling stopped while queries continued running on Snowflake.
    - Bug fix: Fixed Azure GET (download) operations incorrectly reporting `UPLOADED` result status instead of `DOWNLOADED` when the server returns presigned URLs for an encrypted stage.
    - Bug fix: Fixed query context cache not being updated when the server returns `queryContext` in a failed query response.
    - Bug fix: Improved CRL issuer validation: issuer names are now compared using DER encoding (avoiding string-form mismatches such as `S=` vs `ST=`), and the CRL's Authority Key Identifier is verified against the issuing CA's Subject Key Identifier when both extensions are present.
- v5.4.1
    - Extended login-request telemetry with Linux distribution details parsed from `/etc/os-release`
    - Bug fix: Fixed `IndexOutOfRangeException` in Arrow result chunk processing by adding retry state cleanup, batch integrity validation, and defensive bounds checking in `ExtractCell()`.
    - Bug fix: Fixed `IndexOutOfRangeException` when reading `NUMBER`/`DECIMAL` columns with scale > 9 in Arrow result format. The internal powers-of-10 lookup table was too small, causing crashes for high-precision fixed-point types.
- v5.4.0
    - Added support for [DECFLOAT](https://docs.snowflake.com/en/sql-reference/data-types-numeric#decfloat) data type (returned as string to preserve full precision).
    - Bug fix: Fixed `IndexOutOfRangeException` in Arrow result processing when empty batches are returned by Snowflake backend.
- v5.3.0
    - Introduced shared library([source code](https://github.com/snowflakedb/universal-driver/tree/main/sf_mini_core)) for extended telemetry to identify and prepare testing platform for native rust extensions.
- v5.2.1
    - Bug fix: Fix the extremely rare case where intermittent network issues during uploads to Azure Blob Storage prevent metadata updates
- v5.2.0
    - Added multi-targeting support. The appropriate build is selected by NuGet based on target framework and OS.
    - Fixed CRL validation to reject newly downloaded CRLs if their NextUpdate has already expired.
    - Users can now specify non-string values in Toml. For example, `port` can be specified as an integer in the Toml.
    - Add retry for HTTP 307/308 status codes
    - Added exception handling to session heartbeat to prevent network errors from disrupting background heartbeat check.
    - Added support for native arrow structured types.
- v5.1.0
    - Added `APPLICATION_PATH` to `CLIENT_ENVIRONMENT` sent during authentication to identify the application connecting to Snowflake.
    - Renew idle sessions in the pool if keep alive is enabled.
    - Added `CRLDOWNLOADMAXSIZE` connection parameter to limit the maximum size of CRL files downloaded during certificate revocation checks.
    - AWS WIF will now also check the application config and AWS profile credential store when determining the current AWS region
    - Allow users to configure the maximum amount of connections via `SERVICE_POINT_CONNECTION_LIMIT` property.
- v5.0.0
    - Disabled CRL checks by default.
    - Added support for alternative, memory efficient and thread safe CRL (Certificate Revocation List) checks.
    - Support for TLS 1.3 with new parameters MINTLS and MAXTLS controlling protocol version negotiated with the server.
    - Removed `log4net` dependency and enabled delegated logging.
    - Upgraded AWS SDK library to v4.
    - Added the `changelog.yml` GitHub workflow to ensure changelog is updated on release PRs.
    - Removed internal classes from public API.
    - Added support for explicitly setting Azure managed identity client ID via `MANAGED_IDENTITY_CLIENT_ID` environmen
