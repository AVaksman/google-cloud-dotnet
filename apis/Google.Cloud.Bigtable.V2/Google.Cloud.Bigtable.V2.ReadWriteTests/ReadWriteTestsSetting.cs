namespace Google.Cloud.Bigtable.V2.ReadWriteTests
{
    public class ReadWriteTestsSetting
    {
        public string InstanceId { get; set; }
        public string ProjectId { get; set; }
        public string TableName { get; set; }
        public string TablePrefix { get; set; }
        public string ColumnFamily { get; set; }
        public string RowKeyPrefix { get; set; }
        public string LogFile { get; set; }
        public string PerfFile { get; set; }
        public int    Records { get; set; }
        public int    LoadThreads { get; set; }
        public int    RowKeySize { get; set; }
        public int    ColumnLength { get; set; }
        public int    RpcTestDurationMinutes { get; set; }
        public int    RpcThreads { get; set; }
        public int    Columns { get; set; }
        public int    Batchsize { get; set; }
        public int    TestType { get; set; }
        public int    Timeout { get; set; }
    }
}
