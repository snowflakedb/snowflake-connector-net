using System;
using System.IO;
using Snowflake.Data.Configuration;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools;

internal interface IEnvironmentFacade
{
    bool GetBool(EnvVar<bool> envVar);
    int GetInt(EnvVar<int> envVar);
    string GetString(EnvVar<string> envVar);
    string GetFolderPath(Environment.SpecialFolder folder);
    string GetExecutionDirectory();
}

internal sealed class EnvironmentFacade : IEnvironmentFacade
{
    private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<EnvironmentFacade>();

    internal static IEnvironmentFacade Instance { get; } = new EnvironmentFacade();


    public bool GetBool(EnvVar<bool> envVar)
    {
        var result = Extract(envVar, s =>
        {
            var parseResult = bool.TryParse(s, out var parsed);
            return (parseResult, parsed);
        });
        return result;
    }

    public int GetInt(EnvVar<int> envVar)
    {
        var result = Extract(envVar, s =>
        {
            var parseResult = int.TryParse(s, out var parsed);
            return (parseResult, parsed);
        });
        return result;
    }

    public string GetString(EnvVar<string> envVar)
    {
        var result = Extract(envVar, s => (true, s));
        return result;
    }

    public string GetFolderPath(Environment.SpecialFolder folder) => Environment.GetFolderPath(folder);

    public string GetExecutionDirectory()
    {
        var executablePath = Environment.GetCommandLineArgs()[0];
        var directoryName = string.IsNullOrEmpty(executablePath) ? null : Path.GetDirectoryName(executablePath);
        if (string.IsNullOrEmpty(directoryName))
        {
            s_logger.Warn("Unable to determine execution directory");
            return null;
        }
        return directoryName;
    }

    private static T Extract<T>(EnvVar<T> envVar, Func<string, (bool IsSuccess, T Parsed)> extractor)
    {
        string value;
        try
        {
            value = Environment.GetEnvironmentVariable(envVar.Name);
        }
        catch (Exception ex)
        {
            s_logger.Error($"Error when reading {envVar.Name}. Using default value: {envVar.DefaultValue}", ex);
            return envVar.DefaultValue;
        }

        if (string.IsNullOrEmpty(value))
        {
            s_logger.Debug($"Did not retrieve value from {envVar.Name}. Using default value: {envVar.DefaultValue}");
            return envVar.DefaultValue;
        }

        var tryParse = extractor.Invoke(value);
        if (!tryParse.IsSuccess)
        {
            s_logger.Debug($"Provided value {envVar.Name} cannot be parsed properly! Using default value: {envVar.DefaultValue}");
            return envVar.DefaultValue;
        }

        return tryParse.Parsed;
    }
}
