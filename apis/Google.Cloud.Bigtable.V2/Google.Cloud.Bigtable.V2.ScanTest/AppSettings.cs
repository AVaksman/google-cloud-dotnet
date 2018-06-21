namespace Google.Cloud.Bigtable.V2.ScanTest
{
    public class AppSettings
    {
        public string InstanceId { get; set; }
        public string ProjectId { get; set; }
        public string RowKeyPrefix { get; set; }
        public string LogFile { get; set; }
        public string ScanFile { get; set; }
        public long Records { get; set; }
        public long RowsLimit { get; set; }
        public int RowKeySize { get; set; }
        public int ScanTestDurationMinutes { get; set; }
        public string TableName { get; set; }
        public string Title { get; set; }
    }
}
