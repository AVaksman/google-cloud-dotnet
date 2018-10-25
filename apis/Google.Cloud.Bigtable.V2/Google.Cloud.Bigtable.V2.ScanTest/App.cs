using System;
using System.Diagnostics;
using HdrHistogram;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mono.Options;
using System.Threading.Tasks;
using Google.Cloud.Bigtable.V2.ScanTest.Runner;

namespace Google.Cloud.Bigtable.V2.ScanTest
{
    public class App
    {
        #region Private
        private readonly ILogger<App> _logger;
        private readonly AppSettings _config;
        private readonly IScanRunnerService _scanRunnerService;
        #endregion

        public static LongConcurrentHistogram _histogramScan;

        public App(IOptions<AppSettings> config, ILogger<App> logger, IScanRunnerService scanRunnerService)
        {
            _logger = logger;
            _config = config.Value;
            _scanRunnerService = scanRunnerService;
            _histogramScan = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
        }

        public bool ConfigureParser(string[] args)
        {
            var showHelp = false;

            var parser = new OptionSet
            {
                "Options:",
                { "i|instance=", "Bigtable InstanceID name", v => _config.InstanceId = v },
                { "o|timeout=", "MutateRows call timeout setting, default 20000 milliseconds", v => _config.Timeout = Convert.ToInt32(v)},
                { "a|AppProfileId=", "AppProfileId to be used", v => _config.AppProfileId = v },
                { "r|rows=", "RowKey spectrum/total rows loaded", v => _config.Records = Convert.ToInt64(v)},
                { "m|minutes=", "Scan test duration minutes", v => _config.ScanTestDurationMinutes = Convert.ToInt32(v)},
                { "b|batch=", "ReadRows batch size rows", v => _config.RowsLimit = Convert.ToInt64(v)},
                { "t|table=", "Table name", v => _config.TableName = v},
                { "H|Help", "Show this message and exit", v => showHelp = v != null },
            };

            try
            {
                parser.Parse(args);
            }
            catch (OptionException e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Try `-H|--Help' for more information.");
            }

            if (showHelp)
            {
                parser.WriteOptionDescriptions(Console.Out);
            }
            return showHelp;
        }

        public async Task Run()
        {
            _logger.LogInformation($"This is a console application for {_config.Title}");

            try
            {
                var stopwatch = Stopwatch.StartNew();
                var rowCount = await _scanRunnerService.ScanRowsLimit(_histogramScan).ConfigureAwait(false);
                //var rowCount = await _scanRunnerService.Scan(_histogramScan).ConfigureAwait(false);
                stopwatch.Stop();

                _logger.LogInformation($"rowCount {rowCount:N0}, duration: {stopwatch.Elapsed.TotalSeconds:N}");

                _scanRunnerService.WriteCsvToConsole(stopwatch.Elapsed, rowCount, _histogramScan);
                //_scanRunnerService.WriteCsv(stopwatch.Elapsed, rowCount, _histogramScan);
            }

            catch (Exception ex)
            {
                _logger.LogInformation(ex.Message);
            }
        }
    }
}
