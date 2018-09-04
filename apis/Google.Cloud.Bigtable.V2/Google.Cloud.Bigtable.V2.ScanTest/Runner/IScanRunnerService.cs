using System.Threading.Tasks;
using Google.Cloud.Bigtable.V2;
using HdrHistogram;
using System;
using Google.Cloud.Bigtable.Common.V2;

namespace Google.Cloud.Bigtable.V2.ScanTest.Runner
{
    public interface IScanRunnerService
    {
        long ReadErrors { get; set; }
        ReadRowsRequest ReadRowsRequestBuilder(BigtableByteString rowKey);
        void WriteCsvToConsole(TimeSpan scanDuration, int rowsRead,  LongConcurrentHistogram hScan);

        // void WriteCsv(TimeSpan scanDuration, int rowsRead,  LongConcurrentHistogram hScan);

        Task<int> Scan(LongConcurrentHistogram histogramScan);
    }
}