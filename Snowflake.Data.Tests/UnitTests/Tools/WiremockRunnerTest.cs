using System;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public sealed class WiremockRunnerTest : IDisposable
    {
        private readonly HttpClient _httpClient = new(
            new HttpClientHandler
            {
                ClientCertificateOptions = ClientCertificateOption.Manual,
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            }
        );

        private readonly WiremockRunner _runner;

        public WiremockRunnerTest()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        public void Dispose()
        {
            _runner.Dispose();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestRunnerAddMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");

            //act
            var response = await _httpClient.GetAsync(_runner.Url + "/test");

            // assert
            Assert.True(response.IsSuccessStatusCode);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestWiremockResetMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");
            var response = await _httpClient.GetAsync(_runner.Url + "/test");
            Assert.True(response.IsSuccessStatusCode);

            // act
            _runner.ResetMapping();
            response = await _httpClient.GetAsync(_runner.Url + "/test");

            // assert
            Assert.False(response.IsSuccessStatusCode);
        }
    }
}
