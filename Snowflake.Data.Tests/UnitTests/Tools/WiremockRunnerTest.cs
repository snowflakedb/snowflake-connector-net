using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class WiremockRunnerTest
    {
        private WiremockRunner _runner;
        private readonly HttpClient _httpClient = new(
            new HttpClientHandler
            {
                ClientCertificateOptions = ClientCertificateOption.Manual,
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true
            }
        );
        public void BeforeAll()
        {
            _runner = WiremockRunner.NewWiremock();
        }
        public void BeforeEach()
        {
            _runner.ResetMapping();
        }
        public void AfterAll()
        {
            _runner.Stop();
        }

        [Fact]
        public void TestRunnerAddMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");

            //act
            var response = Task.Run(async () => await _httpClient.GetAsync(_runner.WiremockBaseHttpsUrl + "/test")).Result;

            // assert
            Assert.True(response.IsSuccessStatusCode);
        }

        [Fact]
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
