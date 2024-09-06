/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Snowflake.Data.Core
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    class JsonUtils
    {
        /// <summary>
        /// Default serialization settings for JSON serialization and deserialization.
        /// This is to avoid issues when changes are made system wide to the default and keep
        /// our settings locals.
        /// </summary>
        public static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters = { new Array2DConverter() },

        };

        public class AnyToStringJsonConverter : JsonConverter<string>
        {
            public override string Read(ref Utf8JsonReader reader,
                Type typeToConvert,
                JsonSerializerOptions options)
            {

                if (reader.TokenType == JsonTokenType.String)
                {
                    return reader.GetString() ?? String.Empty;
                }
                else if (reader.TokenType == JsonTokenType.Number)
                {
                    var stringValue = reader.GetDouble();
                    return stringValue.ToString();
                }
                else if (reader.TokenType == JsonTokenType.False ||
                         reader.TokenType == JsonTokenType.True)
                {
                    return reader.GetBoolean().ToString();
                }
                else if (reader.TokenType == JsonTokenType.StartObject)
                {
                    reader.Skip();
                    return "(not supported)";
                }
                else
                {

                    throw new System.Text.Json.JsonException();
                }
            }

            public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value);
            }
        }
    }

        public class Array2DConverter : JsonConverterFactory
        {
            public override bool CanConvert(Type typeToConvert) => typeToConvert.IsArray && typeToConvert.GetArrayRank() == 2;

            public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options) =>
                (JsonConverter)Activator.CreateInstance(
                    typeof(Array2DConverterInner<>).MakeGenericType(new [] { type.GetElementType() }),
                    BindingFlags.Instance | BindingFlags.Public,
                    binder: null,
                    args: new object[] { options },
                    culture: null);

            class Array2DConverterInner<T> : JsonConverter<T [,]>
            {
                readonly JsonConverter<T> _valueConverter;

                public Array2DConverterInner(JsonSerializerOptions options) =>
                    this._valueConverter = (typeof(T) == typeof(object) ? null : (JsonConverter<T>)options.GetConverter(typeof(T))); // Encountered a bug using the builtin ObjectConverter

                public override void Write(Utf8JsonWriter writer, T [,] array, JsonSerializerOptions options)
                {
                    // Adapted from this answer https://stackoverflow.com/a/25995025/3744182
                    // By https://stackoverflow.com/users/3258160/pedro
                    // To https://stackoverflow.com/questions/21986909/convert-multidimensional-array-to-jagged-array-in-c-sharp
                    var rowsFirstIndex = array.GetLowerBound(0);
                    var rowsLastIndex = array.GetUpperBound(0);
                    var columnsFirstIndex = array.GetLowerBound(1);
                    var columnsLastIndex = array.GetUpperBound(1);

                    writer.WriteStartArray();
                    for (var i = rowsFirstIndex; i <= rowsLastIndex; i++)
                    {
                        writer.WriteStartArray();
                        for (var j = columnsFirstIndex; j <= columnsLastIndex; j++)
                            _valueConverter.WriteOrSerialize(writer, array[i, j], options);
                        writer.WriteEndArray();
                    }
                    writer.WriteEndArray();
                }

                public override T [,] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                    JsonSerializer.Deserialize<List<List<T>>>(ref reader, options)?.To2D();
            }
    }

        public static partial class ArrayExtensions
        {
            public static T[,] To2D<T>(this List<List<T>> source)
            {
                // Adapted from this answer https://stackoverflow.com/a/26291720/3744182
                // By https://stackoverflow.com/users/3909293/diligent-key-presser
                // To https://stackoverflow.com/questions/26291609/converting-jagged-array-to-2d-array-c-sharp
                var firstDim = source.Count;
                var secondDim = source.Select(row => row.Count).FirstOrDefault();
                if (!source.All(row => row.Count == secondDim))
                    throw new InvalidOperationException();
                var result = new T[firstDim, secondDim];
                for (var i = 0; i < firstDim; i++)
                    for (int j = 0, count = source[i].Count; j < count; j++)
                        result[i, j] = source[i][j];
                return result;
            }
        }

        public static class JsonSerializerExtensions
        {
            public static void WriteOrSerialize<T>(this JsonConverter<T> converter, Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                if (converter != null)
                    converter.Write(writer, value, options);
                else
                    JsonSerializer.Serialize(writer, value, typeof(T), options);
            }
        }
}
