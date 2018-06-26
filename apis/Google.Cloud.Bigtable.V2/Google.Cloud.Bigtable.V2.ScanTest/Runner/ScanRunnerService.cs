using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Grpc.Core;
using HdrHistogram;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Google.Cloud.Bigtable.V2.ScanTest.Runner
{
    internal class ScanRunnerService : IScanRunnerService
    {
        public long ReadErrors { get; set; }

        #region Private
        private readonly ILogger<App> _logger;
        private readonly AppSettings _config;
        #endregion

        static TableName _table;
        static string _stringFormat;

        public ScanRunnerService(ILogger<App> logger, IOptions<AppSettings> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public void WriteCsvToConsole(TimeSpan scanDuration, int rowsRead, LongConcurrentHistogram hScan)
        {
            try
            {
                var throughput = rowsRead / scanDuration.TotalSeconds;
                _logger.LogInformation($"Scan operations={hScan.TotalCount:N0}, failed={ReadErrors:N0}, throughput={throughput:N}, 50th={hScan.GetValueAtPercentile(50) / 100.0:N}, 75th={hScan.GetValueAtPercentile(75) / 100.0:N}, 95th={hScan.GetValueAtPercentile(95) / 100.0:N}, 99th={hScan.GetValueAtPercentile(99) / 100.0:N}, 99.99th={hScan.GetValueAtPercentile(99.99) / 100.0:N}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results to console error: {ex.Message}");
            }
        }

        public void WriteCsv(TimeSpan scanDuration, int rowsRead, LongConcurrentHistogram hScan)
        {
            string path = Directory.GetParent(Directory.GetCurrentDirectory()).ToString() + $"/csv/scan_test_{DateTime.Now:MM_dd_yy_HH_mm}.csv";
            
            try
            {
                var throughput = rowsRead / scanDuration.TotalSeconds;
                using (var writetext = new StreamWriter(path))
                {
                    writetext.WriteLine();
                    writetext.WriteLine(
                        "Stage, Operation Name, Chunk Size, Run Time, Max Latency, Min Latency, Operations, Throughput, p50 latency, p75 latency, p95 latency, p99 latency, p99.99 latency, Success Operations, Failed Operations");
                    writetext.WriteLine(
                        $"Scan , Read, {_config.RowsLimit}, {scanDuration.TotalSeconds:F}, {hScan.Percentiles(5).Max(a => a.ValueIteratedTo) / 100.0}, {hScan.Percentiles(5).Min(a => a.ValueIteratedTo) / 100.0}, {hScan.TotalCount}, {throughput:F0}, {hScan.GetValueAtPercentile(50) / 100.0}, {hScan.GetValueAtPercentile(75) / 100.0}, {hScan.GetValueAtPercentile(95) / 100.0}, {hScan.GetValueAtPercentile(99) / 100.0}, {hScan.GetValueAtPercentile(99.99) / 100.0}, {hScan.TotalCount - ReadErrors}, {ReadErrors}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results into {path}, error: {ex.Message}");

            }
        }

        public async Task<int> Scan(LongConcurrentHistogram histogramScan)
        {
            BigtableClient.ClientCreationSettings _scanClientCreationSettings = new BigtableClient.ClientCreationSettings(_config.ChannelCount);
            BigtableClient _bigtableClient = BigtableClient.Create(_scanClientCreationSettings);

            _stringFormat = "D" + _config.RowKeySize;
            _table = new TableName(_config.ProjectId, _config.InstanceId, _config.TableName);
            //Perform scan test
            var runtime = Stopwatch.StartNew();

            ReadErrors = 0;

            _logger.LogInformation($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting Scan test against table {_config.TableName} for {_config.ScanTestDurationMinutes} minutes at {_config.RowsLimit} rows chunks, using {_config.ChannelCount} channel(s).");

            var rowsRead = 0;
            var r = new Random();

            while (runtime.Elapsed.TotalMinutes < _config.ScanTestDurationMinutes)
            {
                var rowNum = r.Next(0, (int)_config.Records);
                string rowKey = _config.RowKeyPrefix + rowNum.ToString(_stringFormat);

                var readRowsRequest = ReadRowsRequestBuilder(rowKey);

                var startReadTime = runtime.Elapsed;
                try
                {
                    //reading rows by chunks = _settings.RowsLimit at a time
#if DEBUG
                    _logger.LogInformation($"Scanning beginning with rowkey {rowKey}");
#endif
                    var streamingResponse = _bigtableClient.ReadRows(readRowsRequest);
                    rowsRead += await CheckReadAsync(streamingResponse).ConfigureAwait(false);
#if DEBUG
                    var status = streamingResponse.GrpcCall.ResponseHeadersAsync.Status;
                    _logger.LogInformation($"Status = {status}");
#endif
                }
                catch (Exception ex)
                {
                    _logger.LogError($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to read beginning rowkey {rowKey}, error message: {ex.Message}");
                    ReadErrors++;
                }
                finally
                {
                    var endReadTime = runtime.Elapsed;
                    var responseTime = endReadTime - startReadTime;
                    histogramScan.RecordValue((long)(responseTime.TotalMilliseconds * 100));
                }
            }

            return rowsRead;
        }

        public ReadRowsRequest ReadRowsRequestBuilder(BigtableByteString rowKey) =>
            new ReadRowsRequest
            {
                TableNameAsTableName = _table,
                Rows = RowSet.FromRowRanges(RowRange.ClosedOpen(rowKey, null)),
                RowsLimit = _config.RowsLimit,
                Filter = RowFilters.CellsPerColumnLimit(1)
            };

        private async Task<int> CheckReadAsync(ReadRowsStream responseRead)
        {
            int rowCount = 0;
            await responseRead.ForEachAsync(row =>
            {
                rowCount++;
#if DEBUG
 //               _logger.LogInformation($"Read RowKey {row.Key.ToStringUtf8()}");
#endif
            }).ConfigureAwait(false);
            return rowCount;
        }
    }
}
