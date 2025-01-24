# Overview

We would like to be more focused on following the coding conventions popular in .NET community.
Currently the coding style in the driver code is not very consistent, but the goal is to improve it
by following coding convention rules.

# Coding conventions

## Writing code

### Static variables

#### Internal or private static member

Use `s_` prefix + camelCase, eg. `s_someVariable`.

```csharp
public class ExampleClass
{
    private static Something s_someVariable;
    internal static Something s_someInternalVariable;
}
```

#### Internal or private thread static member

Use `t_` prefix + camelCase, eg. `t_someVariable`.

```csharp
public class ExampleClass
{
    [ThreadStatic]
    private static Something t_someVariable;
    
    [ThreadStatic]
    internal static Something t_someInternalVariable;
}
```

#### Public static member

Use PascalCase, eg. `SomeVariable`.

```csharp
public class ExampleClass
{
    public static Something SomeVariable;
}
```
#### Const member

Use always PascalCase regardless of the modifier public/private/internal.

```csharp
public class ExampleClass
{
    public int SomeInteger = 1234;
    private string SomeString = "abc";
    internal string SomeInternalString = "xyz";
}
```

### Object variables

#### Internal or private member

Use `_` prefix + camelCase, eg. `_someVariable`.

```csharp
public class ExampleClass
{
    private Something _someVariable;
    internal Something _someInternalVariable;
}
```

#### Public member

Use PascalCase, eg. `SomeVariable`.

```csharp
public class ExampleClass
{
    public Something SomeVariable;
}
```

#### Property

Use PascalCase, eg. `SomeProperty`.

```csharp
public ExampleProperty
{
    get;
    set;
}
```

### Local variables

Use camelCase, eg. `someVariable`.

```csharp
{
    Something someVariable;
}
```

### Const variables

Use PascalCase, eg. `SomeConst`.

```csharp
{
    const SomeConst = 1;
}
```

### Method names

Use PascalCase, eg. `SomeMethod` for all methods (normal, object members, static members, public, internal, private).

```csharp
void SomeMethod() {
}
```

### Enums

Use PascalCase for both: enumeration name and values, eg. `SomeEnumeration` with value `SomeValue`.

```csharp
public enum SomeEnumeration
{
    SomeValue = 5,
    SomeOtherValue = 7
}
```

### Interface

Use `I` prefix (without `Interface` postfix), eg. `IName`.

```csharp
interface IName
{
}
```

### Class naming

#### Class implementing a standard interface

Use `Snowflake` prefix, eg. `SnowflakeDbCommand` because the class extends `DbCommand` abstract class and implements `IDbCommand` interface.

```csharp
public class SnowflakeDbCommand : DbCommand
{
}
```

#### Class not implementing any standard interface

Don't use any particular prefix if the class does not implement any standard interface. 

```csharp
public class FastParser
{
}
```

## Writing tests

### Arrange, Act, Assert pattern (3a)

If possible split the test code into `arrange`, `act` and `assert` blocks.

```csharp
// arrange
var config = new HttpClientConfig(
    true,
    "snowflake.com",
    "123",
    "testUser",
    "proxyPassword",
    "localhost", 
    false,
    false
);

// act
var handler = (HttpClientHandler) HttpUtil.Instance.SetupCustomHttpHandler(config);

// assert
Assert.IsTrue(handler.UseProxy);
Assert.IsNotNull(handler.Proxy);
```

### TestThatSomethingShouldHappen methods

Use test names in PascalCase notation (but without MS proposed underline characters between logical parts of the test name addressing 3a pattern).

```csharp
[Test]
public void TestThatLoginWithInvalidPassowrdFails() {
}

[Test]
public void TestCreatingHttpClientHandlerWithProxy() {
}
```
