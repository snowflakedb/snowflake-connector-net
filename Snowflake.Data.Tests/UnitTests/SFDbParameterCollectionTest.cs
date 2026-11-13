using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests;

using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using System;
using System.Collections.Generic;
using System.Linq;
public sealed class SFDbParameterCollectionTest
{
    private readonly SnowflakeDbParameterCollection _parameterCollection;
    private const int ParamCount = 10;

    public SFDbParameterCollectionTest()
    {
        _parameterCollection = new SnowflakeDbParameterCollection();
    }

    public static IEnumerable<object[]> AllSFDataTypes() =>
        Enum.GetValues(typeof(SFDataType)).Cast<SFDataType>().Select(v => new object[] { v });

    [SFFact]
    public void TestDefaultDbParameterCollection()
    {
        Assert.Empty(_parameterCollection);
    }

    [SFFact]
    public void TestDbParameterCollectionCount()
    {
        Assert.Empty(_parameterCollection);

        var parameter = new SnowflakeDbParameter();

        for (var i = 0; i < ParamCount; i++)
        {
            _parameterCollection.Add(parameter);
        }

        Assert.Equal(ParamCount, _parameterCollection.Count);
    }

    [SFFact]
    public void TestDbParameterCollectionSyncRoot()
    {
        object obj;
        Assert.Throws<NotImplementedException>(() => obj = _parameterCollection.SyncRoot);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionAddParameter(SFDataType SFDataType)
    {
        var parameter = new SnowflakeDbParameter(1, SFDataType);
        Assert.Equal(0, _parameterCollection.Add(parameter));
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionAddNameAndType(SFDataType SFDataType)
    {
        var parameter = _parameterCollection.Add("1", SFDataType);
        Assert.Equal(parameter, _parameterCollection[0]);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionAddRange(SFDataType SFDataType)
    {
        var parameterArray = new SnowflakeDbParameter[ParamCount];

        for (var i = 0; i < ParamCount; i++)
        {
            parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
        }

        _parameterCollection.AddRange(parameterArray);
        Assert.Equal(ParamCount, _parameterCollection.Count);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionClear(SFDataType SFDataType)
    {
        var parameterArray = new SnowflakeDbParameter[ParamCount];

        for (var i = 0; i < ParamCount; i++)
        {
            parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
        }

        _parameterCollection.AddRange(parameterArray);
        Assert.Equal(ParamCount, _parameterCollection.Count);

        _parameterCollection.Clear();
        Assert.Empty(_parameterCollection);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionContainsName(SFDataType SFDataType)
    {
        var paramName = "1";

        var parameter = new SnowflakeDbParameter(paramName, SFDataType);
        _parameterCollection.Add(parameter);
        Assert.True(_parameterCollection.Contains(paramName));
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionContainsParameter(SFDataType SFDataType)
    {
        var paramName = "1";

        var parameter = new SnowflakeDbParameter(paramName, SFDataType);
        _parameterCollection.Add(parameter);
        Assert.True(_parameterCollection.Contains(parameter));
    }



    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionGetEnumerator(SFDataType SFDataType)
    {
        var parameter = new SnowflakeDbParameter(1, SFDataType);
        _parameterCollection.Add(parameter);

        var parameterEnumerator = _parameterCollection.GetEnumerator();

        parameterEnumerator.Reset();
        Assert.True(parameterEnumerator.MoveNext());
        Assert.Equal(parameter, (SnowflakeDbParameter)parameterEnumerator.Current);
        Assert.False(parameterEnumerator.MoveNext());
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionCopyTo(SFDataType SFDataType)
    {
        var parameter = new SnowflakeDbParameter(1, SFDataType);
        Assert.Equal(0, _parameterCollection.Add(parameter));

        var parameterArray = new SnowflakeDbParameter[0];

        Assert.Throws<NotImplementedException>(() => _parameterCollection.CopyTo(parameterArray, 0));
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionIndexOfName(SFDataType SFDataType)
    {
        var paramName = "9";

        for (var i = 0; i < ParamCount; i++)
        {
            _parameterCollection.Add(new SnowflakeDbParameter(i, SFDataType));
        }

        Assert.Equal(ParamCount - 1, _parameterCollection.IndexOf(paramName));
    }

    [SFFact]
    public void TestDbParameterCollectionIndexOfNameNotExists()
    {
        var expectedParameterIndex = -1;
        var paramName = "1";
        Assert.Equal(expectedParameterIndex, _parameterCollection.IndexOf(paramName));
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionIndexOfValue(SFDataType SFDataType)
    {
        var expectedParameterIndex = 0;
        var parameter = new SnowflakeDbParameter(1, SFDataType);

        _parameterCollection.Add(parameter);
        Assert.Equal(expectedParameterIndex, _parameterCollection.IndexOf(parameter));
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionInsert(SFDataType SFDataType)
    {
        SnowflakeDbParameter parameter;
        for (var expectedParamIndex = 0; expectedParamIndex < ParamCount; expectedParamIndex++)
        {
            parameter = new SnowflakeDbParameter(expectedParamIndex.ToString(), SFDataType);

            _parameterCollection.Insert(expectedParamIndex, parameter);
            Assert.Equal(expectedParamIndex, _parameterCollection.IndexOf(parameter));
        }
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionInsertOutOfBounds(SFDataType SFDataType)
    {
        var indexGreaterThanParameterCollectionSize = _parameterCollection.Count + 1;
        var parameter = new SnowflakeDbParameter(1, SFDataType);
        Assert.Throws<ArgumentOutOfRangeException>(() => _parameterCollection.Insert(indexGreaterThanParameterCollectionSize, parameter));
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionRemove(SFDataType SFDataType)
    {
        var parameterArray = new SnowflakeDbParameter[ParamCount];

        for (var i = 0; i < ParamCount; i++)
        {
            parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
        }

        _parameterCollection.AddRange(parameterArray);
        Assert.Equal(ParamCount, _parameterCollection.Count);


        for (var i = 0; i < ParamCount; i++)
        {
            _parameterCollection.Remove((SnowflakeDbParameter)_parameterCollection[0]);
            Assert.Equal(ParamCount - i - 1, _parameterCollection.Count);
        }
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionRemoveAtName(SFDataType SFDataType)
    {
        var parameterArray = new SnowflakeDbParameter[ParamCount];

        for (var i = 0; i < ParamCount; i++)
        {
            parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
        }

        _parameterCollection.AddRange(parameterArray);
        Assert.Equal(ParamCount, _parameterCollection.Count);

        for (var i = 0; i < ParamCount; i++)
        {
            _parameterCollection.RemoveAt(((SnowflakeDbParameter)_parameterCollection[0]).ParameterName);
            Assert.Equal(ParamCount - i - 1, _parameterCollection.Count);
        }
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionRemoveAtIndex(SFDataType SFDataType)
    {
        var parameterArray = new SnowflakeDbParameter[ParamCount];

        for (var i = 0; i < ParamCount; i++)
        {
            parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
        }

        _parameterCollection.AddRange(parameterArray);
        Assert.Equal(ParamCount, _parameterCollection.Count);


        for (var i = 0; i < ParamCount; i++)
        {
            _parameterCollection.RemoveAt(0);
            Assert.Equal(ParamCount - i - 1, _parameterCollection.Count);
        }
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionGetParameterByName(SFDataType SFDataType)
    {
        var paramName = "1";
        var expectedParameter = new SnowflakeDbParameter(paramName, SFDataType);
        _parameterCollection.Add(expectedParameter);
        Assert.Equal(expectedParameter, _parameterCollection[paramName]);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionGetParameterByIndex(SFDataType SFDataType)
    {
        var paramIndex = 0;
        var expectedParameter = new SnowflakeDbParameter(1, SFDataType);
        _parameterCollection.Add(expectedParameter);
        Assert.Equal(expectedParameter, _parameterCollection[paramIndex]);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionSetParameterByName(SFDataType SFDataType)
    {
        var firstParamName = "1";
        var secondParamName = "2";
        var expectedFirstParameter = new SnowflakeDbParameter(firstParamName, SFDataType);
        _parameterCollection.Add(expectedFirstParameter);
        Assert.Equal(expectedFirstParameter, _parameterCollection[firstParamName]);

        var expectedSecondParameter = new SnowflakeDbParameter(secondParamName, SFDataType);
        _parameterCollection[firstParamName] = expectedSecondParameter;
        Assert.Equal(expectedSecondParameter, _parameterCollection[secondParamName]);
    }

    [SFTheory]
    [MemberData(nameof(AllSFDataTypes))]
    public void TestDbParameterCollectionSetParameterByIndex(SFDataType SFDataType)
    {
        var paramIndex = 0;
        var expectedFirstParameter = new SnowflakeDbParameter(1, SFDataType);
        _parameterCollection.Add(expectedFirstParameter);
        Assert.Equal(expectedFirstParameter, _parameterCollection[paramIndex]);

        var expectedSecondParameter = new SnowflakeDbParameter(2, SFDataType);
        _parameterCollection[paramIndex] = expectedSecondParameter;
        Assert.Equal(expectedSecondParameter, _parameterCollection[paramIndex]);
    }

    [SFFact]
    public void TestDbParameterCollectionTryCastWrongType()
    {
        Assert.False(_parameterCollection.Contains(new SnowflakeDbCommand()));
    }

    [SFFact]
    public void TestDbParameterCollectionTryCastThrowWrongType()
    {
        var ex = Assert.Throws<SnowflakeDbException>(() => _parameterCollection.Add(new SnowflakeDbCommand()));
        Assert.Equal(SFError.UNSUPPORTED_FEATURE.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);

    }
}
