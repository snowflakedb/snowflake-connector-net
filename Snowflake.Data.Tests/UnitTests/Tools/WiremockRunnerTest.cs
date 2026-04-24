using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
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

        [OneTimeSetUp]
        public void BeforeAll()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        [SetUp]
        public void BeforeEach()
        {
            _runner.ResetMapping();
        }

        [OneTimeTearDown]
        public void AfterAll()
        {
            _runner.Dispose();
        }

        [Test]
        public void TestRunnerAddMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");

            //act
            var response = Task.Run(async () => await _httpClient.GetAsync(_runner.SslUrl + "/test")).Result;

            // assert
            Assert.True(response.IsSuccessStatusCode);
        }

        [Test]
        public void TestWiremockResetMapping()
        {
            // arrange
            _runner.AddMappings("wiremock/test_mapping.json");
            var response = Task.Run(async () => await _httpClient.GetAsync(_runner.SslUrl + "/test")).Result;
            Assert.True(response.IsSuccessStatusCode);

            // act
            _runner.ResetMapping();
            response = Task.Run(async () => await _httpClient.GetAsync(_runner.Url + "/__admin/mappings")).Result;

            // assert
            Assert.True(response.IsSuccessStatusCode);
            var json = Task.Run(async () => await response.Content.ReadAsStringAsync()).Result;
            var mappings = JsonConvert.DeserializeObject<dynamic[]>(json);
            Assert.AreEqual(0, mappings.Length);
        }
    }
}
