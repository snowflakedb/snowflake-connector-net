using System;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [CollectionDefinition(nameof(WiremockRunnerTestFixture), DisableParallelization = true)]
    public sealed class WiremockRunnerTestFixture : ICollectionFixture<WiremockRunnerTestFixture.Fixture>
    {
        public sealed class Fixture : IDisposable
        {
            internal WiremockRunner _runner { get; set; }

            public Fixture()
            {
                _runner = WiremockRunner.NewWiremock();
            }

            public void Dispose()
            {
                _runner.Stop();
            }
        }
    }

    [Collection(nameof(WiremockRunnerTestFixture))]
    public class WiremockRunnerTest
    {
        private readonly HttpClient _httpClient = new(
            new HttpClientHandler
            {
                ClientCertificateOptions = ClientCertificateOption.Manual,
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            }
        );

        private readonly WiremockRunner _runner;

        public WiremockRunnerTest(WiremockRunnerTestFixture.Fixture fixture)
        {
            _runner = fixture._runner;
            _runner.ResetMapping();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestRunnerAddMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");

            //act
            var response = Task.Run(async () => await _httpClient.GetAsync(_runner.WiremockBaseHttpsUrl + "/test")).Result;

            // assert
            Assert.True(response.IsSuccessStatusCode);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestWiremockResetMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");
            var response = Task.Run(async () => await _httpClient.GetAsync(_runner.WiremockBaseHttpsUrl + "/test")).Result;
            Assert.True(response.IsSuccessStatusCode);

            // act
            _runner.ResetMapping();
            response = Task.Run(async () => await _httpClient.GetAsync(_runner.WiremockBaseHttpUrl + "/__admin/mappings")).Result;

            // assert
            Assert.True(response.IsSuccessStatusCode);
            dynamic jsonObject = JsonConvert.DeserializeObject(Task.Run(async () => await response.Content.ReadAsStringAsync()).Result);
            Assert.Equal("0", jsonObject?.meta.total.ToString());
        }
    }
}
