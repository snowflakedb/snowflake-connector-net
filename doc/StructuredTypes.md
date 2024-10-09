## Concept

Snowflake structured types documentation is available here: [Snowflake Structured Types Documentation](https://docs.snowflake.com/en/sql-reference/data-types-structured).

Snowflake offers a way to store structured types which can be:
- objects, e.g. ```OBJECT(city VARCHAR, state VARCHAR)```
- arrays, e.g. ```ARRAY(NUMBER)```
- maps, e.g. ```MAP(VARCHAR, VARCHAR)```

The driver allows reading and casting such structured objects into customer classes.

**Note**: Currently, reading structured types is available only for JSON result format.

## Enabling the feature

Currently, reading structured types is available only for JSON result format, so you can make sure you are using JSON result format by:
```sql
ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = JSON;
```

The structured types feature is enabled starting from v4.2.0 driver version.

## Structured types vs semi-structured types

The difference between structured types and semi-structured types is that structured types contain types definitions for given objects/arrays/maps.

E.g. for a given object:
```sql
SELECT OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)
```

The part indicating the type of object is `::OBJECT(city VARCHAR, state VARCHAR)`.
This part of definition is essential for structured types because it is used to convert the object into the customer class instance.

Whereas the corresponding semi-structured type does not contain a detailed type definition, for instance:
```sql
SELECT OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT
```

which means the semi-structured types are returned only as a JSON string.

## Handling objects

You can construct structured objects by using an object constructor and providing type details:

```sql
SELECT OBJECT_CONSTRUCT('city','San Mateo', 'state', 'CA')::OBJECT(city VARCHAR, state VARCHAR)
```

You can read the object into your class by executing `T SnowflakeDbReader.GetObject<T>(int ordinal)` method:

```csharp
var reader = (SnowflakeDbDataReader) command.ExecuteReader();
Assert.IsTrue(reader.Read());
var address = reader.GetObject<Address>(0);
```

where `Address` is a customer class, e.g.
```csharp
public class Address
{
    public string city { get; set; }
    public string state { get; set; }
    public Zip zip { get; set; }
}
```

There are a few possible ways of constructing an object of a customer class.
The customer object (e.g. `Address`) can be created either:
- by the properties order, which is a default method
- by properties names
- by the constructor.

### Creating objects by properties order

Creating objects by properties order is a default construction method.
Objects are created by the non-parametrized constructor, and then the n-th Snowflake object field is converted into the n-th customer object property, one by one.

You can annotate your class with `SnowflakeObject` annotation to make sure this creation method would be chosen (however it is not necessary since it is a default method):
```csharp
[SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.PROPERTIES_ORDER)]
public class Address
{
    public string city { get; set; }
    public string state { get; set; }
    public Zip zip { get; set; }
}
```

If you would like to skip any customer property, you could use a `[SnowflakeColumn(IgnoreForPropertyOrder = true)]` annotation for a given property.
For instance, the annotation used in the following class definition makes the `city` be skipped when mapping the properties:
```csharp
public class Address
{
    [SnowflakeColumn(IgnoreForPropertyOrder = true)]
    public string city { get; set; }
    public string state { get; set; }
    public Zip zip { get; set; }
}
```

So, the first field from the database object would be mapped to the `state` property because `city` is skipped.

### Creating objects by property names

Using the `[SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)]` annotation on the customer class can enable the creation of objects by their property names.
In this creation method, objects are created by the non-parametrised constructor, and then for each of the database object fields a property of the same name is set with the field value.
It is crucial that database object field names are the same as customer property names; otherwise, a given database object field value would not be set in the customer object.
You can use the annotation `SnowflakeColumn` to rename the customer object property to the match database object field name.

In the example:

```csharp
[SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.PROPERTIES_NAMES)]
public class Address
{
    [SnowflakeColumn(Name = "nearestCity")]
    public string city { get; set; }
    public string state { get; set; }
    public Zip zip { get; set; }
}
```

the database object field `nearestCity` would be mapped to the `city` property of `Address` class.

### Creating objects by the constructor

Using the `[SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.CONSTRUCTOR)]` annotation on the customer class enables the creation of objects by a constructor.
In this creation method, an object with all its fields is created by a constructor.
A constructor with the exact number of parameters as the number of database object fields should exist because such a constructor would be chosen to instantiate a customer object.
Database object fields are mapped to customer object constructor parameters based on their order.

Example:
```csharp
[SnowflakeObject(ConstructionMethod = SnowflakeObjectConstructionMethod.CONSTRUCTOR)]
public class Address
{
    private string _city;
    private string _state;

    public Address()
    {
    }

    public Address(string city, string state)
    {
        _city = city;
        _state = state;
    }
}
```

## Handling arrays

You can construct structured arrays like this:

```sql
SELECT ARRAY_CONSTRUCT('a', 'b', 'c')::ARRAY(TEXT)
```

You can read such a structured array using `T[] SnowflakeDbReader.GetArray<T>(int ordinal)` method to get an array of specified type.

```csharp
var reader = (SnowflakeDbDataReader) command.ExecuteReader();
Assert.IsTrue(reader.Read());
var array = reader.GetArray<string>(0);
```

## Handling maps

You can construct structured maps like this:

```sql
SELECT OBJECT_CONSTRUCT('5','San Mateo', '8', 'CA', '13', '01-234')::MAP(INTEGER, VARCHAR)
```

**Note**: The only possible map key types are: VARCHAR or NUMBER with scale 0.

You can read a structured map using `Dictionary<TKey, TValue> SnowflakeDbReader.GetMap<TKey, TValue>(int ordinal)` method to get an array of specified type.

```csharp
var reader = (SnowflakeDbDataReader) command.ExecuteReader();
Assert.IsTrue(reader.Read());
var map = reader.GetMap<int, string>(0);
```
