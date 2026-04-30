# Snowflake Connector for .NET

[![codecov](https://codecov.io/github/snowflakedb/snowflake-connector-net/coverage.svg?branch=master)](https://codecov.io/github/snowflakedb/snowflake-connector-net?branch=master)
[![NuGet](https://img.shields.io/nuget/v/Snowflake.Data.svg)](https://www.nuget.org/packages/Snowflake.Data/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The Snowflake .NET connector supports the following .NET framework and library versions:

- .NET Framework 4.6.2
- .NET Framework 4.7.1
- .NET Framework 4.7.2
- .NET Framework 4.8
- .NET Framework 4.8.1
- .NET 6.0
- .NET 7.0
- .NET 8.0
- .NET 9.0

The connector targets `netstandard2.0` but only the versions listed above are tested and supported.

## Target Frameworks and Platform-Specific Builds

Starting from version **5.2.0**, the connector uses multi-targeting to provide optimized builds for different platforms:

| Target Framework | Platform | Description |
|------------------|----------|-------------|
| `net481` | Windows (.NET Framework 4.8.1) | Optimized build without Mono.Unix |
| `net8.0-windows` | Windows (.NET 8+) | Optimized build without Mono.Unix |
| `net8.0` | Linux, macOS (.NET 8+) | Full Unix file system support with Mono.Unix |
| `netstandard2.0` | All platforms | Backward compatibility for older .NET versions |

NuGet automatically selects the appropriate build based on the application's target framework and operating system.

Please refer to the [Notice](#notice) section for information about safe usage of the .NET Driver.


# Installation

The NuGet package ID is **Snowflake.Data**. Install it via any of the standard methods:

**Package Manager Console:**
```
PM> Install-Package Snowflake.Data
```

**.NET CLI:**
```
dotnet add package Snowflake.Data
```

**Visual Studio UI:** Tools > NuGet Package Manager > Manage NuGet Packages for Solution > search for `Snowflake.Data`.

Packages are also available for direct download from [nuget.org](https://www.nuget.org/packages/Snowflake.Data/).

## Verifying the Package Signature

Starting from version v4.2.0 the driver package is signed, allowing verification of its authenticity and integrity.

1. Install `cosign`
2. Download the driver package file (`.nupkg`) from nuget, e.g.: https://www.nuget.org/packages/Snowflake.Data/4.2.0
3. Download the signatures file from the release, e.g.: https://github.com/snowflakedb/snowflake-connector-net/releases/tag/v4.2.0
4. Verify the signature:
```shell
cosign verify-blob snowflake.data.4.2.0.nupkg \
--key snowflake-connector-net-v4.2.0.pub \
--signature Snowflake.Data.4.2.0.nupkg.sig

Verified OK
```

---

# Usage

Detailed documentation for each area of the driver is maintained in separate pages. The table below links to the relevant guide for each topic.

| Topic | Description | Documentation |
|-------|-------------|---------------|
| Connecting | Connection string parameters and authentication methods | [Connecting](doc/Client/Connecting.md) |
| Connection Pools | Pool lifecycle, sizing, and configuration (v4.0.0+) | [Connection Pooling](doc/Client/ConnectionPooling.md) |
| Data Types | .NET to Snowflake type mapping, Arrow format | [Data Types](doc/Client/DataTypes.md) |
| Querying Data | Sync/async queries, bindings, multi-statement support | [Querying Data](doc/Client/QueryingData.md) |
| Structured Types | Reading objects, arrays, and maps into .NET types | [Structured Types](doc/Client/StructuredTypes.md) |
| Vector Type | Reading `VECTOR(INT)` / `VECTOR(FLOAT)` columns | [Vector Type](doc/Client/VectorType.md) |
| Stage Files | PUT/GET commands for uploading and downloading files | [Stage Files](doc/Client/StageFiles.md) |
| Logging | Built-in and custom `ILogger` integration, easy logging | [Logging](doc/Client/Logging.md) |
| Certificate Validation | CRL-based revocation checks (differs from other Snowflake drivers) | [Certificate Validation](doc/Client/CertficateValidation.md) |
| Cache | Token caching for SSO/MFA/OAuth, CRL cache | [Cache](doc/Client/Cache.md) |

---

## Notice

1.  CVE-2019-0820 -
    This CVE has been reported in systems.text.regularexpressions.dll which is used by the regular expressions packages - systems.text.regularexpressions.4.3.1.nupkg. This vulnerability manifests itself ONLY when the following .NET runtime environments are being used:

        * v1.0 branch: 1.0 - 1.0.16 (exclusive)
        * v1.1 branch: 1.1 - 1.1.13 (exclusive)
        * v2.1 branch: 2.1 - 2.1.11 (exclusive)
        * v2.2 branch: 2.2 - 2.2.5  (exclusive)

        In order to mitigate this vulnerability, we recommend to update to higher Runtime versions. If you're already running on a .NET Runtime version higher than the ones listed above, you're not going to be affected by this vulnerability.

2.  Logging -
    Snowflake has identified an issue on Feb 20, 2020, with our logging code for the .NET drivers in which we write Master and Session tokens in the clear to the debug logs. The debug logs are collected locally on the drive where your programs are running. This issue impacts only those instances where the programs are run with debug flags enabled, i.e. setting the log level value= "Debug" or "All" in the log4Net config

    Under normal conditions, the Master and Session tokens captured in the log files are short-lived for about 4 and 1 hours, respectively. They will expire after the 4-hour window unless explicitly refreshed, in which case they could be refreshed indefinitely.

    If you are using the .NET driver please take the following action:

    - Upgrade to the latest version(v1.1.0) as soon as possible.
    - Remove all "Debugging" options for any existing .NET drivers in use.
    - Delete any logs collected thus far and make sure that all copies are deleted.
    - If you cannot upgrade for any reason, please ensure all debugging is disabled
    - If you are concerned about a potential compromise, contact Snowflake Customer Support for assistance with invalidating all active sessions/tokens.

3.  Global HTTP connection settings -
    Snowflake has identified an issue where the driver is globally enforcing TLS 1.2 and certificate revocation checks with the .NET Driver v1.2.1 and earlier versions.
    Starting with v2.0.0, the driver will set these locally.

4.  Certificate Revocation List not performed where insecureMode was disabled -
    Snowflake has identified vulnerability where the checks against the Certificate Revocation List (CRL)
    were not performed where the insecureMode flag was set to false, which is the default setting.
    From version v2.1.5 CRL is working back as intended.

5.  This driver currently does not support GCP regional endpoints. Please ensure that any workloads using through this driver do not require support for regional endpoints on GCP. If you have questions about this, please contact Snowflake Support.

6. The driver uses Rust library called sf_mini_core, you can find its source code [here](https://github.com/snowflakedb/universal-driver/tree/main/sf_mini_core) 

Note that the driver is now targeting .NET Standard 2.0. When upgrading, you might also need to run "Update-Package -reinstall" to update the dependencies.

See more:
* [Security Policy](SECURITY.md)
* [Security Advisories](../../security/advisories)

---

# Contributing

## Coding Conventions

If you would like to contribute to this project, please get to know coding conventions we would like to follow:
[Coding conventions](CodingConventions.md).

## Building the Package

Install dotnet, git, nuget, and mono (only on Mac).

1. Check out the source code from GitHub:
```bash
git clone git@github.com:snowflakedb/snowflake-connector-net snowflake-connector-net
```

2. Pull down the dependency:
```bash
cd snowflake-connector-net
nuget restore
```

3. Build the solution file
- To build the connector only:
```bash
cd Snowflake.Data
dotnet build --configuration Release
```
- To build the connector and test project:
```
Add a parameters.json file to Snowflake.Data.Tests
dotnet build
```

## Testing and Code Coverage

[Running tests](doc/Library%20Contributions/Testing.md)

[Code coverage](doc/Library%20Contributions/CodeCoverage.md)
