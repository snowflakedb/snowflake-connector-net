using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Internal;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util;

#if NET8_0_OR_GREATER
using System.Threading.Tasks;
using Xunit.v3;

// TODO polish
public sealed class SFTestCaseDiscovererV3 : IXunitTestCaseDiscoverer
{
    public ValueTask<IReadOnlyCollection<IXunitTestCase>> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, IXunitTestMethod testMethod, IFactAttribute factAttribute)
    {
        var retriesCountEnum = (factAttribute as SFFactAttribute)?.RetriesCount ?? RetriesCount.NoRetries;
        var retriesCount = (int)retriesCountEnum;
        var details = TestIntrospectionHelper.GetTestCaseDetails(discoveryOptions, testMethod, factAttribute);
        var testCase = new SFTestCase(maxRetries: retriesCount,
            testMethod: testMethod,
            testCaseDisplayName: details.TestCaseDisplayName,
            uniqueID: details.UniqueID,
            @explicit: details.Explicit,
            skipExceptions: details.SkipExceptions,
            skipReason: details.SkipReason,
            skipType: details.SkipType,
            skipUnless: details.SkipUnless,
            skipWhen: details.SkipWhen,
            testMethod.Traits.ToReadWrite(StringComparer.OrdinalIgnoreCase),
            timeout: details.Timeout
        );

        return new ([testCase]);
    }
}

public sealed class SFTheoryDiscovererV3 : TheoryDiscoverer
{
    protected override ValueTask<IReadOnlyCollection<IXunitTestCase>> CreateTestCasesForDataRow(ITestFrameworkDiscoveryOptions discoveryOptions, IXunitTestMethod testMethod, ITheoryAttribute theoryAttribute,
        ITheoryDataRow dataRow, object[] testMethodArguments)
    {
        var retriesCountEnum = (theoryAttribute as SFTheoryAttribute)?.RetriesCount ?? RetriesCount.NoRetries;
        var retriesCount = (int)retriesCountEnum;
        var details = TestIntrospectionHelper.GetTestCaseDetails(discoveryOptions, testMethod, theoryAttribute, testMethodArguments);
        var testCase = new SFTestCase(
            retriesCount,
            details.ResolvedTestMethod,
            details.TestCaseDisplayName,
            details.UniqueID,
            details.Explicit,
            details.SkipExceptions,
            details.SkipReason,
            details.SkipType,
            details.SkipUnless,
            details.SkipWhen,
            testMethod.Traits.ToReadWrite(StringComparer.OrdinalIgnoreCase),
            testMethodArguments,
            timeout: details.Timeout
        );

        return new([testCase]);
    }

     protected override ValueTask<IReadOnlyCollection<IXunitTestCase>> CreateTestCasesForTheory(
        ITestFrameworkDiscoveryOptions discoveryOptions,
        IXunitTestMethod testMethod,
        ITheoryAttribute theoryAttribute)
    {
        var retriesCountEnum = (theoryAttribute as SFTheoryAttribute)?.RetriesCount ?? RetriesCount.NoRetries;
        var retriesCount = (int)retriesCountEnum;
        var details = TestIntrospectionHelper.GetTestCaseDetails(discoveryOptions, testMethod, theoryAttribute);
        XunitTestCase testCase =
            details.SkipReason is not null && details.SkipUnless is null && details.SkipWhen is null
                // Unconditionally skipped theory should yield a single XunitTestCase which will return a skipped result
                ? new SFTestCase(
                    retriesCount,
                    details.ResolvedTestMethod,
                    details.TestCaseDisplayName,
                    details.UniqueID,
                    details.Explicit,
                    details.SkipExceptions,
                    details.SkipReason,
                    details.SkipType,
                    details.SkipUnless,
                    details.SkipWhen,
                    testMethod.Traits.ToReadWrite(StringComparer.OrdinalIgnoreCase),
                    timeout: details.Timeout
                )
                // Otherwise, return a test case which will enumerate the data later
                : new SFEnumeratedTestCase(
                    retriesCount,
                    details.ResolvedTestMethod,
                    details.TestCaseDisplayName,
                    details.UniqueID,
                    details.Explicit,
                    theoryAttribute.SkipTestWithoutData,
                    details.SkipExceptions,
                    details.SkipReason,
                    details.SkipType,
                    details.SkipUnless,
                    details.SkipWhen,
                    testMethod.Traits.ToReadWrite(StringComparer.OrdinalIgnoreCase),
                    timeout: details.Timeout
                );

        return new([testCase]);
    }
}

#else
using Xunit.Abstractions;

public sealed class SFTestCaseDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly FactDiscoverer _decorated;

    public SFTestCaseDiscoverer(IMessageSink diagnosticMessageSink)
    {
        _decorated = new FactDiscoverer(diagnosticMessageSink);
    }

    public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute) =>
        _decorated.Discover(discoveryOptions, testMethod, factAttribute).Select(x =>
        {
            var retriesCount = (int)((SFFactAttribute)((ReflectionAttributeInfo)factAttribute).Attribute).RetriesCount;
            return new SFTestCases(x, retriesCount);
        });
}

public sealed class SFTheoryDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly TheoryDiscoverer _decorated;

    public SFTheoryDiscoverer(IMessageSink diagnosticMessageSink)
    {
        _decorated = new TheoryDiscoverer(diagnosticMessageSink);
    }

    public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute) =>
        _decorated.Discover(discoveryOptions, testMethod, factAttribute).Select(x =>
        {
            var retriesCount = (int)((SFTheoryAttribute)((ReflectionAttributeInfo)factAttribute).Attribute).RetriesCount;
            return new SFTestCases(x, retriesCount);
        });
}

#endif

