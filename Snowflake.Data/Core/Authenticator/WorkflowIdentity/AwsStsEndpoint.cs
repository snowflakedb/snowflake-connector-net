using System;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity;

/// <summary>
/// An AWS STS endpoint to send an attestation request to: either the regional endpoint derived from the
/// AWS region, or one parsed from a configured host.
/// </summary>
internal sealed class AwsStsEndpoint
{
    private const string DefaultScheme = "https";
    private const string SchemeSeparator = "://";

    /// <summary>
    /// Host, with a port when it is not the scheme's default. Sent as the Host header and covered by the
    /// SigV4 signature, so it must always describe the endpoint the request is actually sent to.
    /// </summary>
    internal string Host { get; }

    /// <summary>
    /// Scheme and host, without a trailing slash, e.g. "https://sts.us-east-1.amazonaws.com".
    /// </summary>
    internal string BaseUrl { get; }

    private AwsStsEndpoint(string host, string baseUrl)
    {
        Host = host;
        BaseUrl = baseUrl;
    }

    /// <summary>
    /// The regional STS endpoint, used when no host is configured.
    /// </summary>
    internal static AwsStsEndpoint ForRegion(string region)
    {
        var domain = region.StartsWith("cn-", StringComparison.Ordinal) ? "amazonaws.com.cn" : "amazonaws.com";
        var host = $"sts.{region}.{domain}";
        return new AwsStsEndpoint(host, $"{DefaultScheme}{SchemeSeparator}{host}");
    }

    /// <summary>
    /// Parses a configured host. It may be a bare host ("sts.sc2s.sgov.gov"), optionally with a port, or
    /// a full URL ("https://sts.sc2s.sgov.gov/"). No partition or domain-suffix mapping is applied - the
    /// host is used as given - but it is normalized: the scheme defaults to https, a port that is the
    /// scheme's default is dropped, and trailing slashes are removed.
    /// <para>
    /// Anything that cannot describe an endpoint is rejected rather than silently ignored, so a
    /// misconfiguration is reported instead of hidden: an unparsable value, a scheme other than
    /// http(s), user info, a query, a fragment, or a path.
    /// </para>
    /// </summary>
    /// <param name="value">The configured host or URL.</param>
    /// <param name="endpoint">The parsed endpoint, or null when the value was rejected.</param>
    /// <param name="problem">
    /// Why the value was rejected, phrased to follow the offending value (e.g. "must not contain a path"),
    /// or null on success. Callers turn this into an error appropriate to where the value came from.
    /// </param>
    /// <returns>True when the value describes a usable endpoint.</returns>
    internal static bool TryParse(string value, out AwsStsEndpoint endpoint, out string problem)
    {
        endpoint = null;
        problem = null;

        var candidate = (value ?? string.Empty).Trim();
        if (candidate.Length == 0)
        {
            problem = "is empty";
            return false;
        }

        if (!candidate.Contains(SchemeSeparator))
        {
            candidate = $"{DefaultScheme}{SchemeSeparator}{candidate}";
        }

        // An absolute http(s) URI cannot have an empty authority, so a missing host is rejected here too.
        if (!Uri.TryCreate(candidate, UriKind.Absolute, out var uri))
        {
            problem = "is not a valid host or URL";
            return false;
        }
        if (uri.Scheme != Uri.UriSchemeHttps && uri.Scheme != Uri.UriSchemeHttp)
        {
            problem = $"must use https or http, got scheme '{uri.Scheme}'";
            return false;
        }
        if (!string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment))
        {
            problem = "must not contain user info, a query or a fragment";
            return false;
        }
        // Only slashes are tolerated as a path. Anything else is rejected instead of carried into the
        // request URL, where a percent-encoded segment such as a trailing "%2F" would survive trimming
        // and turn into an empty path segment once the query is appended.
        if (uri.AbsolutePath.Trim('/').Length != 0)
        {
            problem = "must not contain a path";
            return false;
        }

        var host = uri.IsDefaultPort ? uri.Host : $"{uri.Host}:{uri.Port}";
        endpoint = new AwsStsEndpoint(host, $"{uri.Scheme}{SchemeSeparator}{host}");
        return true;
    }
}
