# Vector type

Vector type represents an array of either integer or float type and a fixed size.
Examples:
- `[4, 5, 6]::VECTOR(INT, 3)` is a 3 elements vector of integers
- `[1.1, 2.2]::VECTOR(FLOAT, 2)` is a 2 elements vector of floats

More about vectors you can read here: [Vector data types](https://docs.snowflake.com/en/sql-reference/data-types-vector).

The driver allows to read a vector column into `int[]` or `float[]` arrays by calling `T[] SnowflakeDbReader.GetArray<T>(int ordinal)`
method for either int or float types.

```csharp
var reader = (SnowflakeDbDataReader) command.ExecuteReader();
Assert.IsTrue(reader.Read());
int[] intVector = reader.GetArray<int>(0);
float[] floatVector = reader.GetArray<float>(1);
```
