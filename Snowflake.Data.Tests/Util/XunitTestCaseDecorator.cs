using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit.Sdk;

#if NET8_0_OR_GREATER
using Xunit.v3;

namespace Snowflake.Data.Tests.Util;

public sealed class XunitTestCaseDecorator : IXunitTestCase
{
    private readonly IXunitTestCase _xunitTestCaseImplementation;

    public XunitTestCaseDecorator(IXunitTestCase xunitTestCaseImplementation)
    {
        _xunitTestCaseImplementation = xunitTestCaseImplementation;
    }

    public bool Explicit => _xunitTestCaseImplementation.Explicit;

    public ValueTask<IReadOnlyCollection<IXunitTest>> CreateTests() => _xunitTestCaseImplementation.CreateTests();

    public void PostInvoke() => _xunitTestCaseImplementation.PostInvoke();

    public void PreInvoke() => _xunitTestCaseImplementation.PreInvoke();

    public Type[] SkipExceptions => _xunitTestCaseImplementation.SkipExceptions;

    public string SkipReason => ((ITestCaseMetadata)_xunitTestCaseImplementation).SkipReason;

    public Type SkipType => _xunitTestCaseImplementation.SkipType;

    public string SkipUnless => _xunitTestCaseImplementation.SkipUnless;

    public string SkipWhen => _xunitTestCaseImplementation.SkipWhen;

    public IXunitTestClass TestClass => _xunitTestCaseImplementation.TestClass;

    public IXunitTestCollection TestCollection => _xunitTestCaseImplementation.TestCollection;

    public IXunitTestMethod TestMethod => _xunitTestCaseImplementation.TestMethod;

    public int TestMethodMetadataToken => _xunitTestCaseImplementation.TestMethodMetadataToken;

    public int TestClassMetadataToken => _xunitTestCaseImplementation.TestClassMetadataToken;

    public string SourceFilePath => _xunitTestCaseImplementation.SourceFilePath;

    public int? SourceLineNumber => _xunitTestCaseImplementation.SourceLineNumber;

    public string TestCaseDisplayName => _xunitTestCaseImplementation.TestCaseDisplayName;

    int? ITestCaseMetadata.TestClassMetadataToken => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestClassMetadataToken;

    public string TestClassName => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestClassName;

    public string TestClassNamespace => _xunitTestCaseImplementation.TestClassNamespace;

    public string TestClassSimpleName => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestClassSimpleName;

    public int? TestMethodArity => _xunitTestCaseImplementation.TestMethodArity;

    int? ITestCaseMetadata.TestMethodMetadataToken => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestMethodMetadataToken;

    public string TestMethodName => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestMethodName;

    public string[] TestMethodParameterTypesVSTest => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestMethodParameterTypesVSTest;

    public string TestMethodReturnTypeVSTest => ((ITestCaseMetadata)_xunitTestCaseImplementation).TestMethodReturnTypeVSTest;

    public int Timeout => _xunitTestCaseImplementation.Timeout;

    public IReadOnlyDictionary<string, IReadOnlyCollection<string>> Traits => _xunitTestCaseImplementation.Traits;

    public string UniqueID => _xunitTestCaseImplementation.UniqueID;

    ITestClass ITestCase.TestClass => ((ITestCase)_xunitTestCaseImplementation).TestClass;

    ITestCollection ITestCase.TestCollection => ((ITestCase)_xunitTestCaseImplementation).TestCollection;

    ITestMethod ITestCase.TestMethod => ((ITestCase)_xunitTestCaseImplementation).TestMethod;
}

#else
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace Snowflake.Data.Tests.Util;


public sealed class XunitTestCaseDecorator : LongLivedMarshalByRefObject, IXunitTestCase
{
    private readonly IXunitTestCase _xunitTestCaseImplementation;

    public XunitTestCaseDecorator()
    {
    }

    public XunitTestCaseDecorator(IXunitTestCase xunitTestCaseImplementation)
    {
        _xunitTestCaseImplementation = xunitTestCaseImplementation;
    }

    public void Deserialize(IXunitSerializationInfo info) => _xunitTestCaseImplementation.Deserialize(info);

    public void Serialize(IXunitSerializationInfo info) => _xunitTestCaseImplementation.Serialize(info);

    public string DisplayName => _xunitTestCaseImplementation.DisplayName;

    public string SkipReason => _xunitTestCaseImplementation.SkipReason;

    public ISourceInformation SourceInformation
    {
        get => _xunitTestCaseImplementation.SourceInformation;
        set => _xunitTestCaseImplementation.SourceInformation = value;
    }

    public ITestMethod TestMethod => _xunitTestCaseImplementation.TestMethod;

    public object[] TestMethodArguments => _xunitTestCaseImplementation.TestMethodArguments;

    public Dictionary<string, List<string>> Traits => _xunitTestCaseImplementation.Traits;

    public string UniqueID => _xunitTestCaseImplementation.UniqueID;

    public async Task<RunSummary> RunAsync(IMessageSink diagnosticMessageSink, IMessageBus messageBus, object[] constructorArguments,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
    {
        var messageBusDecorator = new MessageBusDecorator(messageBus);
        var baseResult = await _xunitTestCaseImplementation
            .RunAsync(diagnosticMessageSink, messageBusDecorator, constructorArguments, aggregator, cancellationTokenSource).ConfigureAwait(false);
        baseResult.Failed -= messageBusDecorator.SkippedCount;
        baseResult.Skipped += messageBusDecorator.SkippedCount;
        return baseResult;
    }

    public Exception InitializationException => _xunitTestCaseImplementation.InitializationException;

    public IMethodInfo Method => _xunitTestCaseImplementation.Method;

    public int Timeout => _xunitTestCaseImplementation.Timeout;
}
#endif
