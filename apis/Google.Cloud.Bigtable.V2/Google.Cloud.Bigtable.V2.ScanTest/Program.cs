using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Google.Cloud.Bigtable.V2.ScanTest.Runner;

namespace Google.Cloud.Bigtable.V2.ScanTest
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var appService = serviceProvider.GetService<App>();
            if (!appService.ConfigureParser(args))
            {
                await appService.Run().ConfigureAwait(false);
            }
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            // add configured instance of logging
            serviceCollection.AddSingleton(new LoggerFactory()
                .AddConsole()
                .AddDebug());

            // add logging
            serviceCollection.AddLogging();

            // build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false)
                .Build();
            serviceCollection.AddOptions();
            serviceCollection.Configure<AppSettings>(configuration.GetSection("Configuration"));

            // add services 
            serviceCollection.AddTransient<IScanRunnerService, ScanRunnerService>();

            // add app
            serviceCollection.AddTransient<App>();

            serviceCollection.AddSingleton<IConfiguration>(configuration);
        }

    }
}
