# Overview

We would like to be more focused on following the coding conventions popular in .NET community.
Currently the coding style in the driver code is not very consistent, but the goal is to improve it
by following coding convention rules.

# Coding conventions

## Writing code

### Static variables

#### Internal or private static member

Use `s_` prefix + CamelCase, eg. `s_someVariable`.

```csharp
public class ExampleClass
{
    private static Something s_someVariable;
    internal static Something s_someVariable2;
}
```

#### Internal or private thread static member

Use `t_` prefix + CamelCase, eg. `t_someVariable`.

```csharp
public class ExampleClass
{
    [ThreadStatic]
    private static Something t_someVariable;
    
    [ThreadStatic]
    internal static Something t_someVariable2;
}
```

#### Public static member

Use Pascal notation, eg. `SomeVariable`.

```csharp
public class ExampleClass
{
    public static Something SomeVariable;
}
```

### Object variables

#### Internal or private member

Use `_` prefix + CamelCase, eg. `_someVariable`.

```csharp
public class ExampleClass
{
    private Something _someVariable;
    internal Something _someVariable2;
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

### Local variables

Use CamelCase, eg. `someVariable`.

```csharp
{
    Something _someVariable;
}
```

### Method names

Use PascalNotation eg. `SomeMethod` for all methods (normal, object members, static members, public, internal, private).

```csharp
void SomeMethod() {
}
```


### Interface

Use `I` prefix (without `Interface` postfix), eg. `IName`.

```csharp
interface IName
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

Use test names in Pascal notation (but without MS proposed underline characters between logical parts of the test name addressing 3a pattern).

```csharp
[Test]
public void TestThatLoginWithInvalidPassowrdFails() {
}

[Test]
public void TestCreatingHttpClientHandlerWithProxy() {
}
```
