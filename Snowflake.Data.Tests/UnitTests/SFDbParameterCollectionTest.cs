namespace Snowflake.Data.Tests
{
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System;
    using System.Collections;
    class SFDbParameterCollectionTest
    {
        SnowflakeDbParameterCollection _parameterCollection;
        const int PARAM_COUNT = 10;
        public void BeforeTest()
        {
            _parameterCollection = new SnowflakeDbParameterCollection();
        }

        [Fact]
        public void TestDefaultDbParameterCollection()
        {
            Assert.Equal(0, _parameterCollection.Count);
        }

        [Fact]
        public void TestDbParameterCollectionCount()
        {
            Assert.Equal(0, _parameterCollection.Count);

            SnowflakeDbParameter parameter = new SnowflakeDbParameter();

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.Add(parameter);
            }

            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);
        }

        [Fact]
        public void TestDbParameterCollectionSyncRoot()
        {
            object obj;
            Assert.Throws<NotImplementedException>(() => obj = _parameterCollection.SyncRoot);
        }

        [Fact]
        public void TestDbParameterCollectionAddParameter(SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(0, _parameterCollection.Add(parameter));
        }

        [Fact]
        public void TestDbParameterCollectionAddNameAndType(SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = _parameterCollection.Add("1", SFDataType);
            Assert.Equal(parameter, _parameterCollection[0]);
        }

        [Fact]
        public void TestDbParameterCollectionAddRange(SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);
        }

        [Fact]
        public void TestDbParameterCollectionClear(SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);

            _parameterCollection.Clear();
            Assert.Equal(0, _parameterCollection.Count);
        }

        [Fact]
        public void TestDbParameterCollectionContainsName(SFDataType SFDataType)
        {
            string paramName = "1";

            SnowflakeDbParameter parameter = new SnowflakeDbParameter(paramName, SFDataType);
            _parameterCollection.Add(parameter);
            Assert.True(_parameterCollection.Contains(paramName));
        }

        [Fact]
        public void TestDbParameterCollectionContainsParameter(SFDataType SFDataType)
        {
            string paramName = "1";

            SnowflakeDbParameter parameter = new SnowflakeDbParameter(paramName, SFDataType);
            _parameterCollection.Add(parameter);
            Assert.True(_parameterCollection.Contains(parameter));
        }



        [Fact]
        public void TestDbParameterCollectionGetEnumerator(SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            _parameterCollection.Add(parameter);

            IEnumerator parameterEnumerator = _parameterCollection.GetEnumerator();

            parameterEnumerator.Reset();
            Assert.True(parameterEnumerator.MoveNext());
            Assert.Equal(parameter, (SnowflakeDbParameter)parameterEnumerator.Current);
            Assert.False(parameterEnumerator.MoveNext());
        }

        [Fact]
        public void TestDbParameterCollectionCopyTo(SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(0, _parameterCollection.Add(parameter));

            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[0];

            Assert.Throws<NotImplementedException>(() => _parameterCollection.CopyTo(parameterArray, 0));
        }

        [Fact]
        public void TestDbParameterCollectionIndexOfName(SFDataType SFDataType)
        {
            string paramName = "9";

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.Add(new SnowflakeDbParameter(i, SFDataType));
            }

            Assert.Equal(PARAM_COUNT - 1, _parameterCollection.IndexOf(paramName));
        }

        [Fact]
        public void TestDbParameterCollectionIndexOfNameNotExists(SFDataType SFDataType)
        {
            int expectedParameterIndex = -1;
            string paramName = "1";
            Assert.Equal(expectedParameterIndex, _parameterCollection.IndexOf(paramName));
        }

        [Fact]
        public void TestDbParameterCollectionIndexOfValue(SFDataType SFDataType)
        {
            int expectedParameterIndex = 0;
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);

            _parameterCollection.Add(parameter);
            Assert.Equal(expectedParameterIndex, _parameterCollection.IndexOf(parameter));
        }

        [Fact]
        public void TestDbParameterCollectionInsert(SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter;
            for (int expectedParamIndex = 0; expectedParamIndex < PARAM_COUNT; expectedParamIndex++)
            {
                parameter = new SnowflakeDbParameter(expectedParamIndex.ToString(), SFDataType);

                _parameterCollection.Insert(expectedParamIndex, parameter);
                Assert.Equal(expectedParamIndex, _parameterCollection.IndexOf(parameter));
            }
        }

        [Fact]
        public void TestDbParameterCollectionInsertOutOfBounds(SFDataType SFDataType)
        {
            int indexGreaterThanParameterCollectionSize = _parameterCollection.Count + 1;
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Throws<ArgumentOutOfRangeException>(() => _parameterCollection.Insert(indexGreaterThanParameterCollectionSize, parameter));
        }

        [Fact]
        public void TestDbParameterCollectionRemove(SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);


            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.Remove((SnowflakeDbParameter)_parameterCollection[0]);
                Assert.Equal(PARAM_COUNT - i - 1, _parameterCollection.Count);
            }
        }

        [Fact]
        public void TestDbParameterCollectionRemoveAtName(SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.RemoveAt(((SnowflakeDbParameter)_parameterCollection[0]).ParameterName);
                Assert.Equal(PARAM_COUNT - i - 1, _parameterCollection.Count);
            }
        }

        [Fact]
        public void TestDbParameterCollectionRemoveAtIndex(SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);


            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.RemoveAt(0);
                Assert.Equal(PARAM_COUNT - i - 1, _parameterCollection.Count);
            }
        }

        [Fact]
        public void TestDbParameterCollectionGetParameterByName(SFDataType SFDataType)
        {
            string paramName = "1";
            SnowflakeDbParameter expectedParameter = new SnowflakeDbParameter(paramName, SFDataType);
            _parameterCollection.Add(expectedParameter);
            Assert.Equal(expectedParameter, _parameterCollection[paramName]);
        }

        [Fact]
        public void TestDbParameterCollectionGetParameterByIndex(SFDataType SFDataType)
        {
            int paramIndex = 0;
            SnowflakeDbParameter expectedParameter = new SnowflakeDbParameter(1, SFDataType);
            _parameterCollection.Add(expectedParameter);
            Assert.Equal(expectedParameter, _parameterCollection[paramIndex]);
        }

        [Fact]
        public void TestDbParameterCollectionSetParameterByName(SFDataType SFDataType)
        {
            string firstParamName = "1";
            string secondParamName = "2";
            SnowflakeDbParameter expectedFirstParameter = new SnowflakeDbParameter(firstParamName, SFDataType);
            _parameterCollection.Add(expectedFirstParameter);
            Assert.Equal(expectedFirstParameter, _parameterCollection[firstParamName]);

            SnowflakeDbParameter expectedSecondParameter = new SnowflakeDbParameter(secondParamName, SFDataType);
            _parameterCollection[firstParamName] = expectedSecondParameter;
            Assert.Equal(expectedSecondParameter, _parameterCollection[secondParamName]);
        }

        [Fact]
        public void TestDbParameterCollectionSetParameterByIndex(SFDataType SFDataType)
        {
            int paramIndex = 0;
            SnowflakeDbParameter expectedFirstParameter = new SnowflakeDbParameter(1, SFDataType);
            _parameterCollection.Add(expectedFirstParameter);
            Assert.Equal(expectedFirstParameter, _parameterCollection[paramIndex]);

            SnowflakeDbParameter expectedSecondParameter = new SnowflakeDbParameter(2, SFDataType);
            _parameterCollection[paramIndex] = expectedSecondParameter;
            Assert.Equal(expectedSecondParameter, _parameterCollection[paramIndex]);
        }

        [Fact]
        public void TestDbParameterCollectionTryCastWrongType()
        {
            Assert.False(_parameterCollection.Contains(new SnowflakeDbCommand()));
        }

        [Fact]
        public void TestDbParameterCollectionTryCastThrowWrongType()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _parameterCollection.Add(new SnowflakeDbCommand()));
            Assert.Equal(SFError.UNSUPPORTED_FEATURE.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);

        }
    }
}
