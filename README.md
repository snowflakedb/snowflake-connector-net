# Snowflake Connector for .NET

[![codecov](https://codecov.io/github/snowflakedb/snowflake-connector-net/coverage.svg?branch=master)](https://codecov.io/github/snowflakedb/snowflake-connector-net?branch=master)
[![NuGet](https://img.shields.io/nuget/v/Snowflake.Data.svg)](https://www.nuget.org/packages/Snowflake.Data/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The Snowflake .NET connector supports the following .NET framework and libraries versions:

- .NET Framework 4.6.2
- .NET Framework 4.7.1
- .NET Framework 4.7.2
- .NET Framework 4.8
- .NET Framework 4.8.1
- .NET 6.0
- .NET 7.0
- .NET 8.0
- .NET 9.0

Disclaimer: While the connector targets netstandard2.0 and may work with versions in its [support matrix](https://learn.microsoft.com/en-us/dotnet/standard/net-standard?tabs=net-standard-2-0#select-net-standard-version), only the versions listed above are supported and tested by the connector

## Target Frameworks and Platform-Specific Builds

Starting from version **5.2.0**, the Snowflake .NET connector uses multi-targeting to provide optimized builds for different platforms:

| Target Framework | Platform | Description                                                 |
|------------------|----------|-------------------------------------------------------------|
| `net481` | Windows (.NET Framework 4.8.1) | Optimized build for Windows .NET Framework without Mono.Unix |
| `net8.0-windows` | Windows (.NET 8+) | Optimized build for Windows .NET 8+ without Mono.Unix |
| `net8.0` | Linux, macOS (.NET 8+) | Full Unix file system support with Mono.Unix                |
| `netstandard2.0` | All platforms | Backward compatibility for older .NET versions              |

**What this means for you:**

- **Windows users** on .NET Framework 4.8.1 will receive the `net481` build without the `Mono.Unix` dependency.
- **Windows users** on .NET 8 or higher will receive the `net8.0-windows` build without the `Mono.Unix` dependency.
- **Linux and macOS users** on .NET 8 or higher will receive the `net8.0`.
- **Older .NET versions** (including older .NET Framework and .NET versions) will use the `netstandard2.0` build for backward compatibility.

The appropriate build is automatically selected by NuGet based on your application's target framework and operating system.

Please refer to the [Notice](#notice) section below for information about safe usage of the .NET Driver

# Coding conventions for the project

If you would like to contribute to this project, please get to know coding conventions we would like to follow:
[Coding conventions](CodingConventions.md).

# Building the Package

You can build Snowflake .NET connector applications for Window, Macintosh, and Linux operating systems. For information about supported operating system versions, refer to the [Client Versions and Support Policy](https://docs.snowflake.com/release-notes/requirements).

## Prerequisites

This project is developed under Visual Studio 2017. Earlier versions of Visual Studio are not supported.

## Steps

Prerequisites: Install dotnet, git, nuget, and mono (Only on Mac)

1. Check out the source code from GitHub:

```{r, engine='bash', code_block_name}
git clone git@github.com:snowflakedb/snowflake-connector-net snowflake-connector-net
```

2. Pull down the dependency:

```{r, engine='bash', code_block_name}
cd snowflake-connector-net
nuget restore
```

3. Build the solution file
- To build the connector only:
```{r, engine='bash', code_block_name}
cd Snowflake.Data
dotnet build --configuration Release
```
- To build the connector and test project:
```
Add a parameters.json file to Snowflake.Data.Tests
dotnet build
```

# Installing the Package

Package ID for Snowflake Connector for .Net is Snowflake.Data.

Packages can be directly downloaded from [nuget.org](https://www.nuget.org/).

It can also be downloaded using Visual Studio UI (Tools > NuGet Package Manager > Manage NuGet Packages for Solution and search for "Snowflake.Data")

Alternatively, packages can also be downloaded using Package Manager Console:

```{r, engine='bash', code_block_name}
PM> Install-Package Snowflake.Data
```

# Verifying the package signature

Starting from version v4.2.0 the driver package is signed with a signature allowing to verify its authenticity and integrity.
Steps to verify the signature:
1. Install `cosign`
2. Download the driver package file (`.nupkg`) from nuget, e.g.: https://www.nuget.org/packages/Snowflake.Data/4.2.0
3. Download the signatures file from the release, e.g.: https://github.com/snowflakedb/snowflake-connector-net/releases/tag/v4.2.0
4. Verify the signature, e.g:
```shell
cosign verify-blob snowflake.data.4.2.0.nupkg \
--key snowflake-connector-net-v4.2.0.pub \
--signature Snowflake.Data.4.2.0.nupkg.sig

Verified OK
```

# Testing and Code Coverage

[Running tests](doc/Testing.md)

[Code coverage](doc/CodeCoverage.md)

---

# Usage

## Create a Connection

To create a connection get familiar with: [Connecting and Authentication Methods](doc/Connecting.md)

## Using Connection Pools

Connection pooling description: [Multiple Connection Pools](doc/ConnectionPooling.md).

Pooling prior to v4.0.0 is described: [Single Connection Pool](doc/ConnectionPoolingDeprecated.md) - `deprecated`

## Data Types and Formats

Snowflake data types and their .NET types is covered in: [Data Types and Data Formats](doc/DataTypes.md)

## Querying Data

How execute a query, use query bindings, run queries synchronously and asynchronously:
[Running Queries and Reading Results](doc/QueryingData.md)

## Structured types

Using structured types: [Structured types](doc/StructuredTypes.md)

## Vector type

Using vector type: [Vector type](doc/VectorType.md)

## Stage Files

Using stage files within PUT/GET commands:
[PUT and GET Files to/from Stage](doc/StageFiles.md)

## Logging

Logging description and configuration:
[Logging and Easy Logging](doc/Logging.md)

## Certificate validation
Method of validating the connection's certificates in the .NET driver differs from the rest of the Snowflake drivers.
Read more in [certificate validation](doc/CertficateValidation.md) docs.

## Cache

Storing tokens in cache for SSO/MFA authentication.

Read more in [cache](doc/Cache.md) docs.

---------------

## Notice

1.  CVE-2019-0820 -
    This CVE has been reported in systems.text.regularexpressions.dll which is used by the regular expressions packages - systems.text.regularexpressions.4.3.1.nupkg. This vulnerability manifests itself ONLY when the following .NET runtime environments are being used:

        * v1.0 branch: 1.0 - 1.0.16 (exclusive)
        * v1.1 branch: 1.1 - 1.1.13 (exclusive)
        * v2.1 branch: 2.1 - 2.1.11 (exclusive)
        * v2.2 branch: 2.2 - 2.2.5  (exclusive)

    In order to mitigate this vulnerability, we recommend to update to higher Runtime versions. If you're already running on a .NET Runtime version higher than the ones listed above, you're not going to be affected by this vulnerability.

2.  Logging -
    Snowflake has identified an issue on Feb 20, 2020, with our logging code for the .NET drivers in which we write Master and Session tokens in the clear to the debug logs. The debug logs are collected locally on the drive where your programs are running. This issue impacts only those instances where the programs are run with debug flags enabled, i.e. setting the log level value= "Debug” or “All" in the log4Net config

    Under normal conditions, the Master and Session tokens captured in the log files are short-lived for about 4 and 1 hours, respectively. They will expire after the 4-hour window unless explicitly refreshed, in which case they could be refreshed indefinitely.

    If you are using the .NET driver please take the following action:

    - Upgrade to the latest version(v1.1.0) as soon as possible.
    - Remove all “Debugging” options for any existing .NET drivers in use.
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

Note that the driver is now targeting .NET Standard 2.0. When upgrading, you might also need to run “Update-Package -reinstall” to update the dependencies.

See more:
* [Security Policy](SECURITY.md)
* [Security Advisories](../../security/advisories)
