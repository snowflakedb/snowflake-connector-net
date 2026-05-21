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

        [SetUp]
        public void BeforeTest()
        {
            _parameterCollection = new SnowflakeDbParameterCollection();
        }

        [SFFact]
        public void TestDefaultDbParameterCollection()
        {
            Assert.Zero(_parameterCollection.Count);
        }

        [SFFact]
        public void TestDbParameterCollectionCount()
        {
            Assert.Zero(_parameterCollection.Count);

            SnowflakeDbParameter parameter = new SnowflakeDbParameter();

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.Add(parameter);
            }

            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);
        }

        [SFFact]
        public void TestDbParameterCollectionSyncRoot()
        {
            object obj;
            Assert.Throws<NotImplementedException>(() => obj = _parameterCollection.SyncRoot);
        }

        [SFFact]
        public void TestDbParameterCollectionAddParameter([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Zero(_parameterCollection.Add(parameter));
        }

        [SFFact]
        public void TestDbParameterCollectionAddNameAndType([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = _parameterCollection.Add("1", SFDataType);
            Assert.Equal(parameter, _parameterCollection[0]);
        }

        [SFFact]
        public void TestDbParameterCollectionAddRange([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);
        }

        [SFFact]
        public void TestDbParameterCollectionClear([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[PARAM_COUNT];

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                parameterArray[i] = new SnowflakeDbParameter(i, SFDataType);
            }

            _parameterCollection.AddRange(parameterArray);
            Assert.Equal(PARAM_COUNT, _parameterCollection.Count);

            _parameterCollection.Clear();
            Assert.Zero(_parameterCollection.Count);
        }

        [SFFact]
        public void TestDbParameterCollectionContainsName([Values] SFDataType SFDataType)
        {
            string paramName = "1";

            SnowflakeDbParameter parameter = new SnowflakeDbParameter(paramName, SFDataType);
            _parameterCollection.Add(parameter);
            Assert.True(_parameterCollection.Contains(paramName));
        }

        [SFFact]
        public void TestDbParameterCollectionContainsParameter([Values] SFDataType SFDataType)
        {
            string paramName = "1";

            SnowflakeDbParameter parameter = new SnowflakeDbParameter(paramName, SFDataType);
            _parameterCollection.Add(parameter);
            Assert.True(_parameterCollection.Contains(parameter));
        }



        [SFFact]
        public void TestDbParameterCollectionGetEnumerator([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            _parameterCollection.Add(parameter);

            IEnumerator parameterEnumerator = _parameterCollection.GetEnumerator();

            parameterEnumerator.Reset();
            Assert.True(parameterEnumerator.MoveNext());
            Assert.Equal(parameter, (SnowflakeDbParameter)parameterEnumerator.Current);
            Assert.False(parameterEnumerator.MoveNext());
        }

        [SFFact]
        public void TestDbParameterCollectionCopyTo([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Zero(_parameterCollection.Add(parameter));

            SnowflakeDbParameter[] parameterArray = new SnowflakeDbParameter[0];

            Assert.Throws<NotImplementedException>(() => _parameterCollection.CopyTo(parameterArray, 0));
        }

        [SFFact]
        public void TestDbParameterCollectionIndexOfName([Values] SFDataType SFDataType)
        {
            string paramName = "9";

            for (int i = 0; i < PARAM_COUNT; i++)
            {
                _parameterCollection.Add(new SnowflakeDbParameter(i, SFDataType));
            }

            Assert.Equal(PARAM_COUNT - 1, _parameterCollection.IndexOf(paramName));
        }

        [SFFact]
        public void TestDbParameterCollectionIndexOfNameNotExists([Values] SFDataType SFDataType)
        {
            int expectedParameterIndex = -1;
            string paramName = "1";
            Assert.Equal(expectedParameterIndex, _parameterCollection.IndexOf(paramName));
        }

        [SFFact]
        public void TestDbParameterCollectionIndexOfValue([Values] SFDataType SFDataType)
        {
            int expectedParameterIndex = 0;
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);

            _parameterCollection.Add(parameter);
            Assert.Equal(expectedParameterIndex, _parameterCollection.IndexOf(parameter));
        }

        [SFFact]
        public void TestDbParameterCollectionInsert([Values] SFDataType SFDataType)
        {
            SnowflakeDbParameter parameter;
            for (int expectedParamIndex = 0; expectedParamIndex < PARAM_COUNT; expectedParamIndex++)
            {
                parameter = new SnowflakeDbParameter(expectedParamIndex.ToString(), SFDataType);

                _parameterCollection.Insert(expectedParamIndex, parameter);
                Assert.Equal(expectedParamIndex, _parameterCollection.IndexOf(parameter));
            }
        }

        [SFFact]
        public void TestDbParameterCollectionInsertOutOfBounds([Values] SFDataType SFDataType)
        {
            int indexGreaterThanParameterCollectionSize = _parameterCollection.Count + 1;
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Throws<ArgumentOutOfRangeException>(() => _parameterCollection.Insert(indexGreaterThanParameterCollectionSize, parameter));
        }

        [SFFact]
        public void TestDbParameterCollectionRemove([Values] SFDataType SFDataType)
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

        [SFFact]
        public void TestDbParameterCollectionRemoveAtName([Values] SFDataType SFDataType)
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

        [SFFact]
        public void TestDbParameterCollectionRemoveAtIndex([Values] SFDataType SFDataType)
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

        [SFFact]
        public void TestDbParameterCollectionGetParameterByName([Values] SFDataType SFDataType)
        {
            string paramName = "1";
            SnowflakeDbParameter expectedParameter = new SnowflakeDbParameter(paramName, SFDataType);
            _parameterCollection.Add(expectedParameter);
            Assert.Equal(expectedParameter, _parameterCollection[paramName]);
        }

        [SFFact]
        public void TestDbParameterCollectionGetParameterByIndex([Values] SFDataType SFDataType)
        {
            int paramIndex = 0;
            SnowflakeDbParameter expectedParameter = new SnowflakeDbParameter(1, SFDataType);
            _parameterCollection.Add(expectedParameter);
            Assert.Equal(expectedParameter, _parameterCollection[paramIndex]);
        }

        [SFFact]
        public void TestDbParameterCollectionSetParameterByName([Values] SFDataType SFDataType)
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

        [SFFact]
        public void TestDbParameterCollectionSetParameterByIndex([Values] SFDataType SFDataType)
        {
            int paramIndex = 0;
            SnowflakeDbParameter expectedFirstParameter = new SnowflakeDbParameter(1, SFDataType);
            _parameterCollection.Add(expectedFirstParameter);
            Assert.Equal(expectedFirstParameter, _parameterCollection[paramIndex]);

            SnowflakeDbParameter expectedSecondParameter = new SnowflakeDbParameter(2, SFDataType);
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
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _parameterCollection.Add(new SnowflakeDbCommand()));
            Assert.Equal(SFError.UNSUPPORTED_FEATURE.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);

        }
    }
}
