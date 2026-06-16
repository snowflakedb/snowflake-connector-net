using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Util.WiremockModels;
using WireMock;
using WireMock.Matchers;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;
using WireMock.Settings;
using Mapping = Snowflake.Data.Tests.Util.WiremockModels.Mapping;

namespace Snowflake.Data.Tests.Util
{
    public interface IWiremockRunner : IDisposable
    {
        string WiremockBaseHttpUrl { get; }
        void Stop();
        void ResetMapping();
        void AddMappings(string file, StringTransformations transformations = null);
        IRequestMessage[] GetCapturedRequests();
    }

    public sealed class WiremockRunner : IWiremockRunner
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WiremockRunner>();
        private WireMockServer _server;

        public string Url => _server.Urls.First(u => u.StartsWith("http://"));
        public string WiremockBaseHttpUrl => Url;

        public static WiremockRunner NewWiremock(string[] mappingFiles = null) =>
            NewWiremockAsync(mappingFiles).GetAwaiter().GetResult();

        public IRequestMessage[] GetCapturedRequests() => _server.LogEntries.Select(x => x.RequestMessage).ToArray();

        private static async Task<WiremockRunner> NewWiremockAsync(string[] mappingFiles = null)
        {
            var runner = new WiremockRunner();
            const string Localhost = "127.0.0.1";
            var httpPort = FindFreePort();
            var httpsPort = FindFreePort();
            runner._server = WireMockServer.Start(new WireMockServerSettings
            {
                Urls = [$"http://{Localhost}:{httpPort}"],
                StartAdminInterface = true
            });
            s_logger.Debug($"WireMock started at {runner.Url}.");

            if (mappingFiles == null)
                return runner;

            foreach (var mappingFile in mappingFiles)
            {
                runner.AddMappings(mappingFile);
            }

            return runner;
        }

        public void AddMappings(string file, StringTransformations transformations = null)
        {
            s_logger.Debug($"Adding wiremock mappings from {file}");
            var json = File.ReadAllText(file);
            var transformed = (transformations ?? StringTransformations.NoTransformationsInstance).Transform(json);

            var root = JsonConvert.DeserializeObject<MappingFile>(transformed);
            var mappings = root?.Mappings ?? new List<Mapping> { JsonConvert.DeserializeObject<Mapping>(transformed) };

            foreach (var mapping in mappings)
            {
                AddSingleMapping(mapping);
            }

            s_logger.Debug($"WireMock mappings added from {file}");
        }

        private void AddSingleMapping(Mapping mapping)
        {
            var request = BuildRequest(mapping.Request);
            var response = BuildResponse(mapping.Response);

            if (mapping.ScenarioName != null)
            {
                var builder = _server.Given(request).InScenario(mapping.ScenarioName);
                if (mapping.WhenStateIs != null)
                    builder = builder.WhenStateIs(mapping.WhenStateIs);
                if (mapping.SetStateTo != null)
                    builder.WillSetStateTo(mapping.SetStateTo);

                builder.RespondWith(response);
            }
            else
            {
                _server.Given(request).RespondWith(response);
            }
        }

        private static IRequestBuilder BuildRequest(MappingRequest req)
        {
            if (req == null) return Request.Create();

            var builder = Request.Create();

            if (req.UrlPathPattern != null)
                builder = builder.WithPath(new RegexMatcher(req.UrlPathPattern));
            else if (req.UrlPattern != null)
                builder = builder.WithPath(new RegexMatcher(req.UrlPattern));

            if (req.Method != null)
                builder = builder.UsingMethod(req.Method);

            if (req.Headers != null)
            {
                foreach (var kvp in req.Headers)
                {
                    var matchers = BuildMatchers(kvp.Value);
                    if (matchers != null)
                        builder = builder.WithHeader(kvp.Key, matchers);
                }
            }

            if (req.QueryParameters != null)
            {
                foreach (var kvp in req.QueryParameters)
                {
                    var matchers = BuildMatchers(kvp.Value);
                    if (matchers != null)
                        builder = builder.WithParam(kvp.Key, matchers);
                }
            }

            if (req.BodyPatterns != null)
            {
                builder = req.BodyPatterns.Select(BuildBodyMatcher)
                    .Where(bodyMatcher => bodyMatcher != null)
                    .Aggregate(builder, (current, bodyMatcher) => current.WithBody(bodyMatcher));
            }

            return builder;
        }

        private static IStringMatcher[] BuildMatchers(MatcherSpec spec)
        {
            if (spec == null) return null;

            if (spec.EqualTo != null) return [new ExactMatcher(spec.EqualTo)];
            if (spec.Contains != null) return [new WildcardMatcher($"*{spec.Contains}*")];
            if (spec.Matches != null) return [new RegexMatcher(spec.Matches)];

            return null;
        }

        private static IMatcher BuildBodyMatcher(BodyPattern pattern)
        {
            if (pattern.MatchesJsonPath != null) return new JsonPathMatcher(pattern.MatchesJsonPath);
            if (pattern.Contains != null) return new WildcardMatcher($"*{pattern.Contains}*");
            if (pattern.EqualToJson != null) return new JsonMatcher(pattern.EqualToJson);

            return null;
        }

        private static IResponseBuilder BuildResponse(MappingResponse resp)
        {
            if (resp == null) return Response.Create();

            var builder = Response.Create();

            if (resp.Status != null)
                builder = builder.WithStatusCode(resp.Status.Value);

            if (resp.Headers != null)
            {
                foreach (var kvp in resp.Headers)
                {
                    builder = builder.WithHeader(kvp.Key, kvp.Value);
                }
            }

            if (resp.JsonBody != null)
                builder = builder.WithBody(resp.JsonBody, "application/json");

            if (resp.Body != null)
                builder = builder.WithBody(resp.Body);

            if (resp.FixedDelayMilliseconds != null)
                builder = builder.WithDelay(resp.FixedDelayMilliseconds.Value);

            return builder;
        }

        public void ResetMapping() => _server.ResetMappings();

        public void Stop()
        {
            if (_server == null)
                return;

            _server.Stop();
            _server.Dispose();
            _server = null;
        }

        public void Dispose() => Stop();

        private static int FindFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
