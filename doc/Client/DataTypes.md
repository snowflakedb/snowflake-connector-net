## Data Types and Formats

## Mapping .NET and Snowflake Data Types

The .NET driver supports the following mappings from .NET to Snowflake data types.

| .NET Data Type | Data Type in Snowflake |
| -------------- | ---------------------- |
| `int`, `long`  | `NUMBER(38, 0)`        |
| `decimal`      | `NUMBER(38, <scale>)`  |
| `double`       | `REAL`                 |
| `string`       | `TEXT`                 |
| `bool`         | `BOOLEAN`              |
| `byte`         | `BINARY`               |
| `datetime`     | `DATE`                 |

## Arrow data format

The .NET connector, starting with v2.1.3, supports the [Arrow data format](https://arrow.apache.org/)
as a [preview](https://docs.snowflake.com/en/release-notes/preview-features) feature for data transfers
between Snowflake and a .NET client. The Arrow data format avoids extra
conversions between binary and textual representations of the data. The Arrow
data format can improve performance and reduce memory consumption in clients.

The data format is controlled by the
DOTNET_QUERY_RESULT_FORMAT parameter. To use Arrow format, execute:

```snowflake
-- at the session level
ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = ARROW;
-- or at the user level
ALTER USER SET DOTNET_QUERY_RESULT_FORMAT = ARROW;
-- or at the account level
ALTER ACCOUNT SET DOTNET_QUERY_RESULT_FORMAT = ARROW;
```

The valid values for the parameter are:

- ARROW
- JSON (default)

