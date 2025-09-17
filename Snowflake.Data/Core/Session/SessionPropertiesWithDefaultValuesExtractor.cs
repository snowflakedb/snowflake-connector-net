using System;
using System.Linq;
using System.Text.RegularExpressions;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{
    internal class SessionPropertiesWithDefaultValuesExtractor
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SessionPropertiesWithDefaultValuesExtractor>();
        private static readonly Regex s_timeoutFormatRegex = new Regex(@"^(-)?[0-9]{1,10}[mM]?[sS]?$");

        private readonly SFSessionProperties _propertiesDictionary;
        private readonly bool _failOnWrongValue;

        public SessionPropertiesWithDefaultValuesExtractor(SFSessionProperties propertiesDictionary, bool failOnWrongValue)
        {
            _propertiesDictionary = propertiesDictionary;
            _failOnWrongValue = failOnWrongValue;
        }

        public bool ExtractBooleanWithDefaultValue(SFSessionProperty property) =>
            ExtractPropertyWithDefaultValue(
                property,
                Boolean.Parse,
                s => true,
                b => true
            );

        public int ExtractPositiveIntegerWithDefaultValue(
            SFSessionProperty property) =>
            ExtractPropertyWithDefaultValue(
                property,
                int.Parse,
                s => true,
                i => i > 0
            );

        public int ExtractNonNegativeIntegerWithDefaultValue(
            SFSessionProperty property) =>
            ExtractPropertyWithDefaultValue(
                property,
                int.Parse,
                s => true,
                i => i >= 0
            );

        public TimeSpan ExtractTimeout(
            SFSessionProperty property) =>
            ExtractPropertyWithDefaultValue(
                property,
                ExtractTimeout,
                ValidateTimeoutFormat,
                t => true
            );

        public T ExtractPropertyWithDefaultValue<T>(
            SFSessionProperty property,
            Func<string, T> extractor,
            Func<string, bool> preExtractValidation,
            Func<T, bool> postExtractValidation)
        {
            var propertyAttribute = property.GetAttribute<SFSessionPropertyAttr>();
            var defaultValueString = propertyAttribute.defaultValue;
            var defaultValue = extractor(defaultValueString);
            if (!postExtractValidation(defaultValue))
            {
                throw new Exception($"Invalid default value of {property}");
            }
            var valueString = _propertiesDictionary[property];
            if (string.IsNullOrEmpty(valueString))
            {
                s_logger.Warn($"Parameter {property} not defined. Using a default value: {defaultValue}");
                return defaultValue;
            }
            if (!preExtractValidation(valueString))
            {
                return handleFailedValidation(defaultValue, valueString, property);
            }
            T value;
            try
            {
                value = extractor(valueString);
            }
            catch (Exception e)
            {
                if (_failOnWrongValue)
                {
                    s_logger.Error($"Invalid value of parameter {property}. Error: {e}");
                    throw new Exception($"Invalid value of parameter {property}", e);
                }
                s_logger.Warn($"Invalid value of parameter {property}. Using a default a default value: {defaultValue}");
                return defaultValue;
            }
            if (!postExtractValidation(value))
            {
                return handleFailedValidation(defaultValue, value, property);
            }
            return value;
        }

        private TResult handleFailedValidation<TResult, TValue>(
            TResult defaultValue,
            TValue value,
            SFSessionProperty property)
        {
            if (_failOnWrongValue)
            {
                s_logger.Error($"Invalid value of parameter {property}: {value}");
                throw new Exception($"Invalid value of parameter {property}");
            }
            s_logger.Warn($"Invalid value of parameter {property}. Using a default value: {defaultValue}");
            return defaultValue;
        }

        private static bool ValidateTimeoutFormat(string value) =>
            !string.IsNullOrEmpty(value) && s_timeoutFormatRegex.IsMatch(value);

        private static TimeSpan ExtractTimeout(string value)
        {
            var numericValueString = string.Concat(value.Where(IsNumberOrMinus));
            var unitValue = value.Substring(numericValueString.Length).ToLower();
            var numericValue = int.Parse(numericValueString);
            if (numericValue < 0)
                return TimeoutHelper.Infinity();
            switch (unitValue)
            {
                case "":
                case "s":
                    return TimeSpan.FromSeconds(numericValue);
                case "ms":
                    return TimeSpan.FromMilliseconds(numericValue);
                case "m":
                    return TimeSpan.FromMinutes(numericValue);
                default:
                    throw new Exception($"unknown timeout unit value: {unitValue}");
            }
        }

        private static bool IsNumberOrMinus(char value) => char.IsNumber(value) || value.Equals('-');
    }
}
