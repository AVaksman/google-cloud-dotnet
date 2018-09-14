using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Grpc.Auth;
using Grpc.Core;
using HdrHistogram;

namespace Google.Cloud.Bigtable.V2.ReadWriteTests
{
    class ReadWriteTests
    {
        internal static string Type { get; set; }
        internal TableName _table;
        private readonly ReadWriteTestsSetting _settings;
        private readonly object lockObj = new object();
        private static readonly Random _rnd = new Random();
        private static BigtableTableAdminClient _bigtableTableAdminClient;
        private static BigtableClient _bigtableClient;
        private static HistogramBase testHistogram;
        private static HistogramBase testHistogramR;
        private static HistogramBase testHistogramW;
        public static BigtableByteString[] Rand;
        private const int RandSize = 256;
        private string _stringFormat = "D7";
        private int failCtr;
#if DEBUG
        private int _recordsWritten = 0;
#endif

        internal ReadWriteTests(ReadWriteTestsSetting settings)
        {
            _settings = settings;

            switch (_settings.TestType)
            {
                case int n when n <= 5:
                {
                    _bigtableClient = BigtableClient.Create();
                    break;
                }
                case int n when n <= 10:
                {
                    var _mockClientCreationSettings = new BigtableClient.ClientCreationSettings(1, null, ChannelCredentials.Insecure, new ServiceEndpoint("localhost", 50051));
                    _bigtableClient = BigtableClient.Create(_mockClientCreationSettings);
                    _settings.InstanceId = "MockServer";
                    break;
                }
                case int n when n <= 14:
                {
                    break;
                }
                case int n when n > 14:
                {
                    CreateAppProfile();
                    int channelsNeeded;
                    if (n > 17)
                    {
                        channelsNeeded = n == 18
                            ? (settings.LoadThreads + 3) / 4
                            : (Math.Max(settings.RpcThreads, settings.LoadThreads) + 3) / 4;
                        _bigtableTableAdminClient = BigtableTableAdminClient.Create();
                    }
                    else
                    {
                        channelsNeeded = (Math.Max(settings.RpcThreads, settings.LoadThreads) + 3) / 4;
                    }
                    var channelPoolCreationSettings = new BigtableClient.ClientCreationSettings(channelsNeeded, credentials: GetCredentials());
                    _bigtableClient = BigtableClient.Create(channelPoolCreationSettings, settings.AppProfileId);
                    break;
                }
            }
            SetTableName();
        }

        internal void CreateAppProfile()
        {
            BigtableInstanceAdminClient bigtableInstanceAdminClient = BigtableInstanceAdminClient.Create();

            GetAppProfileRequest getAppProfileRequest = new GetAppProfileRequest
            {
                AppProfileName = new AppProfileName(_settings.ProjectId, _settings.InstanceId, _settings.AppProfileId)
            };
            try
            {
                bigtableInstanceAdminClient.GetAppProfile(getAppProfileRequest);
            }
            catch (RpcException ex)
            {
                if (ex.StatusCode == StatusCode.NotFound)
                {
                    AppProfile cSharpAppProfile = new AppProfile
                    {
                        Description = "C# performance testing",
                        MultiClusterRoutingUseAny = new AppProfile.Types.MultiClusterRoutingUseAny()
                    };
                    CreateAppProfileRequest createAppProfileRequest = new CreateAppProfileRequest
                    {
                        ParentAsInstanceName = new InstanceName(_settings.ProjectId, _settings.InstanceId),
                        AppProfileId = _settings.AppProfileId,
                        AppProfile = cSharpAppProfile,
                    };
                    bigtableInstanceAdminClient.CreateAppProfile(createAppProfileRequest);
                }
                else
                {
                    throw;
                }
            }
        }

        internal void WriteOnlyTest()
        {
            testHistogram = new LongHistogram(3, TimeStamp.Hours(1), 3);
            Type = "writeOnly";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using single thread");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} AppProfile ID: {_settings.AppProfileId}");

            var runtime = Stopwatch.StartNew();

            while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
            {
                int rowNum = _rnd.Next(0, _settings.Records);
                BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);

                var request = new MutateRowRequest
                {
                    TableNameAsTableName = _table,
                    RowKey = rowKey.Value,
                    Mutations = { MutationsBuilder() }
                };

#if DEBUG
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Inserting rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                var startInsertTime = runtime.Elapsed;

                try
                {
                    _bigtableClient.MutateRow(request);
                }
                catch (Exception ex)
                {
                    failCtr++;
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to insert rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                }
                finally
                {
                    var endInsertTime = runtime.Elapsed;
                    var responseTime = endInsertTime - startInsertTime;
                    testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                }
            }
        }

        internal void WriteOnlyTestCSharp()
        {
            testHistogram = new LongHistogram(3, TimeStamp.Hours(1), 3);
            Type = "writeOnlyC#";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test for {_settings.RpcTestDurationMinutes} minutes using single thread");

            var runtime = Stopwatch.StartNew();

            while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
            {
                var startInsertTime = runtime.Elapsed;

                int rowNum = _rnd.Next(0, _settings.Records);
                BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);

                var request = new MutateRowRequest
                {
                    TableNameAsTableName = _table,
                    RowKey = rowKey.Value,
                    Mutations = { MutationsBuilder() }
                };

                var endInsertTime = runtime.Elapsed;
                var responseTime = endInsertTime - startInsertTime;
                testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
            }
        }

        internal void WriteOnlyTestMultipleThreads()
        {
            testHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "writeOnlyMultipleThreads";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} threads");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} AppProfile ID: {_settings.AppProfileId}");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);

                        var request = new MutateRowRequest
                        {
                            TableNameAsTableName = _table,
                            RowKey = rowKey.Value,
                            Mutations = { MutationsBuilder() }
                        };
#if DEBUG
                        Console.WriteLine(
                            $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Inserting rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                        var startInsertTime = runtime.Elapsed;

                        try
                        {
                            _bigtableClient.MutateRow(request);
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref failCtr);
                            Console.WriteLine(
                                $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to insert rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                        }
                        finally
                        {
                            var endInsertTime = runtime.Elapsed;
                            var responseTime = endInsertTime - startInsertTime;
                            testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        internal void WriteOnlyTestMultipleChannels()
        {
            testHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "writeOnlyMultipleChannels";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} threads");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} AppProfile ID: {_settings.AppProfileId}");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);

                        var request = new MutateRowRequest
                        {
                            TableNameAsTableName = _table,
                            RowKey = rowKey.Value,
                            Mutations = { MutationsBuilder() }
                        };

#if DEBUG
                        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Inserting rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                        var startInsertTime = runtime.Elapsed;

                        try
                        {
                            _bigtableClient.MutateRow(request, CallSettings.FromCallTiming(CallTiming.FromTimeout(TimeSpan.FromMilliseconds(_settings.Timeout))));
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref failCtr);
                            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startInsertTime).TotalMilliseconds} milliseconds\nFailed to insert rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                        }
                        finally
                        {
                            var endInsertTime = runtime.Elapsed;
                            var responseTime = endInsertTime - startInsertTime;
                            testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        internal void ReadOnlyTest()
        {
            testHistogram = new LongHistogram(3, TimeStamp.Hours(1), 3);
            Type = "readOnly";

            //_table = new TableName(_settings.ProjectId, _settings.InstanceId, _settings.TableName);

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using single thread");

            var runtime = Stopwatch.StartNew();

            while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
            {
                int rowNum = _rnd.Next(0, _settings.Records);
                BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);
#if DEBUG
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Reading rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                var startReadTime = runtime.Elapsed;

                try
                {
                    _bigtableClient.ReadRow(_table, rowKey, new RowFilter { CellsPerColumnLimitFilter = 1 });
                }
                catch (Exception ex)
                {
                    failCtr++;
                    Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to read rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                }
                finally
                {
                    var endInsertTime = runtime.Elapsed;
                    var responseTime = endInsertTime - startReadTime;
                    testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                }
            }
        }

        internal void ReadOnlyTestMultipleThreads()
        {
            testHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "readOnlyMultipleThreads";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} thread");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {

                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);
#if DEBUG
                        Console.WriteLine(
                            $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Reading rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                        var startReadTime = runtime.Elapsed;

                        try
                        {
                            _bigtableClient.ReadRow(_table, rowKey, new RowFilter { CellsPerColumnLimitFilter = 1 });
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref failCtr);
                            Console.WriteLine(
                                $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to read rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                        }
                        finally
                        {
                            var endInsertTime = runtime.Elapsed;
                            var responseTime = endInsertTime - startReadTime;
                            testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        internal void ReadOnlyTestMultipleChannels()
        {
            testHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "readOnlyMultipleChannels";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} thread");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);
#if DEBUG
                        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Reading rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                        var startReadTime = runtime.Elapsed;

                        try
                        {
                            _bigtableClient.ReadRow(_table, rowKey, new RowFilter { CellsPerColumnLimitFilter = 1 });
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref failCtr);
                            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to read rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                        }
                        finally
                        {
                            var endInsertTime = runtime.Elapsed;
                            var responseTime = endInsertTime - startReadTime;
                            testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        internal void ReadWriteMutlipleThreads()
        {
            testHistogramR = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            testHistogramW = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "ReadWriteMultipleThreads";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} thread");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} AppProfile ID: {_settings.AppProfileId}");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {

                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);
#if DEBUG
                        Console.WriteLine(
                            $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Reading rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                        if (rowNum % 2 == 0)
                        {
                            var startReadTime = runtime.Elapsed;

                            try
                            {
                                _bigtableClient.ReadRow(_table, rowKey, new RowFilter { CellsPerColumnLimitFilter = 1 });
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine(
                                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to read rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endReadTime = runtime.Elapsed;
                                var responseTime = endReadTime - startReadTime;
                                testHistogramR.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                        else
                        {
                            var request = new MutateRowRequest
                            {
                                TableNameAsTableName = _table,
                                RowKey = rowKey.Value,
                                Mutations = { MutationsBuilder() }
                            };

#if DEBUG
                            Console.WriteLine(
                                $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Inserting rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                            var startInsertTime = runtime.Elapsed;

                            try
                            {
                                _bigtableClient.MutateRow(request);
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine(
                                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Failed to insert rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endInsertTime = runtime.Elapsed;
                                var responseTime = endInsertTime - startInsertTime;
                                testHistogramW.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        internal void ReadOnlyTestCSharp()
        {
            testHistogram = new LongHistogram(3, TimeStamp.Hours(1), 3);
            Type = "readOnlyC#";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test for {_settings.RpcTestDurationMinutes} minutes using single thread");

            var runtime = Stopwatch.StartNew();

            while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
            {
                var startReadTime = runtime.Elapsed;

                int rowNum = _rnd.Next(0, _settings.Records);
                BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);
                ReadRowsRequest readRowsRequest = new ReadRowsRequest
                {
                    Filter = RowFilters.CellsPerColumnLimit(1),
                    Rows = RowSet.FromRowKey(rowKey),
                    TableNameAsTableName = _table
                };

                var endInsertTime = runtime.Elapsed;
                var responseTime = endInsertTime - startReadTime;
                testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
            }
        }

        internal void LoadTest()
        {
            testHistogram = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "loadTest";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.Records} records using {_settings.LoadThreads} threads");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} AppProfile ID: {_settings.AppProfileId}");

            var startIdCount = _settings.Records / _settings.Batchsize;
            var startIdsList = new List<int>();

            for (int i = 0; i < startIdCount; i++)
            {
                startIdsList.Add(i * _settings.Batchsize);
            }
            startIdsList.Reverse();
            var startIds = new System.Collections.Concurrent.ConcurrentBag<int>(startIdsList);

            using (var IdIterator = startIds.GetEnumerator())
            {
                List<Thread> threads = new List<Thread>();

                var runtime = Stopwatch.StartNew();

                for (int i = 0; i < _settings.LoadThreads; i++)
                {
                    threads.Add(new Thread(() =>
                    {
                        while (IdIterator.MoveNext())
                        {
                            var startId = IdIterator.Current;
                            BigtableByteString rowKey;
                            MutateRowsRequest request = new MutateRowsRequest
                            {
                                TableNameAsTableName = _table
                            };

                            for (int j = 0; j < _settings.Batchsize && startId < _settings.Records; j++)
                            {
                                rowKey = _settings.RowKeyPrefix + startId++.ToString(_stringFormat);
                                request.Entries.Add(Mutations.CreateEntry(rowKey, MutationsBuilder()));
                            }

                            var startTime = runtime.Elapsed;

                            try
                            {
#if DEBUG
                            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} inserting record {startId} last rowkey {rowKey.Value.ToStringUtf8()} for table {_settings.TableName}");
#endif
                                var streamingResponse = _bigtableClient.MutateRows(request);
#if DEBUG
                                _recordsWritten += streamingResponse.Entries.Count;
                                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} task inserted {streamingResponse.Entries.Count} rows, total {_recordsWritten}");
#endif

                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine(
                                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startTime).TotalMilliseconds} milliseconds\n Exception inserting record {startId} last rowkey {rowKey.Value.ToStringUtf8()} for table {_settings.TableName}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endTime = runtime.Elapsed;
                                var responseTime = endTime - startTime;
                                testHistogram.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                    }));
                }
                threads.ForEach(a => a.Start());
                threads.ForEach(a => a.Join());
            }
        }

        internal void FillRandomData()
        {
            Byte[] b = new Byte[_settings.ColumnLength];
            Rand = new BigtableByteString[RandSize];
            for (var i = 0; i < Rand.Length; i++)
            {
                _rnd.NextBytes(b);
                Rand[i] = new BigtableByteString(b);
            }
        }

        internal void SetTableName()
        {
            _table = new TableName(_settings.ProjectId, _settings.InstanceId, _settings.TableName);
        }

        internal void CreateTable()
        {
            try
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Creating table {_settings.TableName} with column family {_settings.ColumnFamily}, Cluster: {_settings.InstanceId}");

                _bigtableTableAdminClient.CreateTable(
                    new InstanceName(_settings.ProjectId, _settings.InstanceId),
                    _settings.TableName,
                    new Table
                    {
                        Granularity = Table.Types.TimestampGranularity.Millis,
                        ColumnFamilies =
                        {
                            {
                                _settings.ColumnFamily, new ColumnFamily
                                {
                                    GcRule = new GcRule
                                    {
                                        MaxNumVersions = 1
                                    }
                                }
                            }
                        }
                    });

                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Table {_settings.TableName} created successfull.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception while creating table {_settings.TableName}. Error Message: " + ex.Message);
            }
        }

        internal void DeleteTable()
        {
            try
            {
                _bigtableTableAdminClient.DeleteTable(
                    new TableName(_settings.ProjectId, _settings.InstanceId, _settings.TableName));

                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Table {_settings.TableName} deleted successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception while deleting table {_settings.TableName}, error message: {ex.Message}");
            }
        }

        internal string GetRandomTableName()
        {
            return _settings.TablePrefix + Path.GetRandomFileName().Substring(0, 8);
        }

        internal void MutationsBuilderTest()
        {
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting MutationsBuilderTest using main thread");
            var ops = 0;
            var runtime = Stopwatch.StartNew();

            while (ops++ <= 10000000)
            {
                var mutations = MutationsBuilder();
            }
            runtime.Stop();
            var totalSeconds = runtime.Elapsed.TotalSeconds;
            var totalMilliseconds = runtime.Elapsed.TotalMilliseconds;
            Console.WriteLine($"MutationsBuilder on main thread built 10,000,000 mutations in {totalSeconds:N2} seconds or {1000000 / totalSeconds:N2} per second or {1000000 / totalMilliseconds:N2} per millisecond");
            Console.WriteLine($"It takes {totalMilliseconds / 1000000} milliseconds to build one mutattion");
        }

        internal void MutationsBuilderTestMultiThread()
        {
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting MutationsBuilderTestMultiThread using {_settings.RpcThreads} threads");
            int ops = 0;
            var runtime = Stopwatch.StartNew();

            List<Thread> threads = new List<Thread>();

            for (int i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (Interlocked.Increment(ref ops) - 1 <= 10000000)
                    {
                        var mutations = MutationsBuilder();
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
            runtime.Stop();
            var totalSeconds = runtime.Elapsed.TotalSeconds;
            var totalMilliseconds = runtime.Elapsed.TotalMilliseconds;
            Console.WriteLine($"MutationsBuilderTestMultiThread using {_settings.RpcThreads} threads built 10,000,000 mutations in {totalSeconds:N2} seconds or {1000000 / totalSeconds:N2} per second or {1000000 / totalMilliseconds:N2} per millisecond");
            Console.WriteLine($"It takes {totalMilliseconds / 1000000} milliseconds to build one mutattion");
        }

        internal void RandomPerformanceTest()
        {
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting RandomPerformanceTest using main thread");
            var ops = 0;
            var runtime = Stopwatch.StartNew();
            while (ops++ <= 10000000)
            {
                int rndIndex = GetRandom(RandSize);
                var x = Rand[(rndIndex + ops) & 0xff];
            }
            runtime.Stop();
            var totalSeconds = runtime.Elapsed.TotalSeconds;
            var totalMilliseconds = runtime.Elapsed.TotalMilliseconds;
            Console.WriteLine($"Random on main thread generated 10,000,000 random numbers in {totalSeconds} seconds == {totalMilliseconds} milliseconds\n" +
                              $"Random efficiemcy is {1000000 / totalSeconds:N2} per second or {1000000 / totalMilliseconds:N2} per millisecond");
            Console.WriteLine($"It takes {runtime.Elapsed.Ticks / 1000000d} ticks (ticks/10000 = milliseconds) to generate one random number");
        }

        internal void RandomPerformanceTestMultiThread()
        {
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss}RandomPerformanceTestMultiThread Starting RandomPerformanceTestMultiThread using {_settings.RpcThreads} threads");
            var ops = 0;
            var runtime = Stopwatch.StartNew();

            List<Thread> threads = new List<Thread>();

            for (int i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (Interlocked.Increment(ref ops) - 1 <= 10000000)
                    {
                        int rndIndex = GetRandom(RandSize);
                        var x = Rand[(rndIndex + ops) & 0xff];
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
            runtime.Stop();
            var totalSeconds = runtime.Elapsed.TotalSeconds;
            var totalMilliseconds = runtime.Elapsed.TotalMilliseconds;
            Console.WriteLine($"Random using {_settings.RpcThreads} threads generated 10,000,000 random numbers in {totalSeconds} seconds == {totalMilliseconds} milliseconds\n" +
                              $"Random efficiemcy is {1000000 / totalSeconds:N2} per second or {1000000 / totalMilliseconds:N2} per millisecond");
            Console.WriteLine($"It takes {runtime.Elapsed.Ticks / 1000000d} ticks (ticks/10000 = milliseconds) to generate one random number");
        }

        internal void WriteCsvToConsole(double duration)
        {
            //var duration = _settings.RpcTestDurationMinutes * 60d;
            try
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} test is finished.");
                Console.WriteLine(
                    $"{Type} operations={testHistogram.TotalCount:N0}, throughput={testHistogram.TotalCount / duration:N2}, 50th={testHistogram.GetValueAtPercentile(50) / 10000d}, 75th={testHistogram.GetValueAtPercentile(75) / 10000d}, 95th={testHistogram.GetValueAtPercentile(95) / 10000d}, 99th={testHistogram.GetValueAtPercentile(99) / 10000d}, 99.9th={testHistogram.GetValueAtPercentile(99.9) / 10000d}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results to console, error: {ex.Message}");
            }
        }

        internal void WriteCsvToConsoleReadWrite(double duration)
        {
            //var duration = _settings.RpcTestDurationMinutes * 60d;
            try
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} test is finished.");
                Console.WriteLine(
                    $"Read operations={testHistogramR.TotalCount:N0}, throughput={testHistogramR.TotalCount / duration:N2}, 50th={testHistogramR.GetValueAtPercentile(50) / 10000d}, 75th={testHistogramR.GetValueAtPercentile(75) / 10000d}, 95th={testHistogramR.GetValueAtPercentile(95) / 10000d}, 99th={testHistogramR.GetValueAtPercentile(99) / 10000d}, 99.9th={testHistogramR.GetValueAtPercentile(99.9) / 10000d}");
                Console.WriteLine(
                    $"Write operations={testHistogramW.TotalCount:N0}, throughput={testHistogramW.TotalCount / duration:N2}, 50th={testHistogramW.GetValueAtPercentile(50) / 10000d}, 75th={testHistogramW.GetValueAtPercentile(75) / 10000d}, 95th={testHistogramW.GetValueAtPercentile(95) / 10000d}, 99th={testHistogramW.GetValueAtPercentile(99) / 10000d}, 99.9th={testHistogramW.GetValueAtPercentile(99.9) / 10000d}");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results to console, error: {ex.Message}");
            }
        }

        internal void WriteCsv(double duration)
        {
            try
            {
                string path = Directory.GetParent(Directory.GetCurrentDirectory()).ToString() + $"/csv/scan_test_{DateTime.Now:MM_dd_yy_HH_mm}.csv";

                using (var writetext = new StreamWriter(path))
                {
                    //var duration = _settings.RpcTestDurationMinutes * 60d;
                    writetext.WriteLine(
                        "Stage, Opration Name,Run Time,Max Latency,Min Latency,Operations,Throughput,p50 latency,p75 latency, p90 latency,p95 latency,p99 latency,p99.9 latency,Success Operations,Failed Operations");
                    writetext.WriteLine(
                        $"Run, Overall time,{duration:F}");
                    writetext.WriteLine(
                        $"Run, {Type}, {duration:F}, {testHistogram.Percentiles(5).Select(a => a.ValueIteratedTo).Max() / 10000d}, {testHistogram.Percentiles(5).Select(a => a.ValueIteratedTo).Min() / 10000d}, {testHistogram.TotalCount}, {testHistogram.TotalCount / duration:F}, {testHistogram.GetValueAtPercentile(50) / 10000d}, {testHistogram.GetValueAtPercentile(75) / 10000d}, {testHistogram.GetValueAtPercentile(90) / 10000d}, {testHistogram.GetValueAtPercentile(95) / 10000d}, {testHistogram.GetValueAtPercentile(99) / 10000d}, {testHistogram.GetValueAtPercentile(99.9) / 10000d}, {testHistogram.TotalCount - failCtr}, {failCtr} ");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results into scan_test_{DateTime.Now}:MM_dd_yy.csv,error: {ex.Message}");
            }
        }

        internal void WriteCsvReadWrite(double duration)
        {
            try
            {
                string path = Directory.GetParent(Directory.GetCurrentDirectory()).ToString() + $"/csv/scan_test_{DateTime.Now:MM_dd_yy_HH_mm}.csv";

                var totalOps = testHistogramR.TotalCount + testHistogramW.TotalCount;
                var readFraction = (double)testHistogramR.TotalCount / totalOps;
                var writeFraction = (double)testHistogramW.TotalCount / totalOps;

                using (var writetext = new StreamWriter(path))
                {
                    //var duration = _settings.RpcTestDurationMinutes * 60d;
                    writetext.WriteLine(
                        "Stage, Opration Name,Run Time,Max Latency,Min Latency,Operations,Throughput,p50 latency,p75 latency, p90 latency,p95 latency,p99 latency,p99.9 latency,Success Operations,Failed Operations");
                    writetext.WriteLine(
                        $"Run, Overall time,{duration:F}");
                    writetext.WriteLine(
                        $"Run, Read, {duration * readFraction:F}, {testHistogramR.Percentiles(5).Select(a => a.ValueIteratedTo).Max() / 10000d}, {testHistogramR.Percentiles(5).Select(a => a.ValueIteratedTo).Min() / 10000d}, {testHistogramR.TotalCount}, {testHistogramR.TotalCount / duration:F}, {testHistogramR.GetValueAtPercentile(50) / 10000d}, {testHistogramR.GetValueAtPercentile(75) / 10000d}, {testHistogramR.GetValueAtPercentile(90) / 10000d}, {testHistogramR.GetValueAtPercentile(95) / 10000d}, {testHistogramR.GetValueAtPercentile(99) / 10000d}, {testHistogramR.GetValueAtPercentile(99.9) / 10000d}, {testHistogramR.TotalCount - failCtr}, {failCtr} ");
                    writetext.WriteLine(
                        $"Run, Write, {duration * writeFraction:F}, {testHistogramW.Percentiles(5).Select(a => a.ValueIteratedTo).Max() / 10000d}, {testHistogramW.Percentiles(5).Select(a => a.ValueIteratedTo).Min() / 10000d}, {testHistogramW.TotalCount}, {testHistogramW.TotalCount / duration:F}, {testHistogramW.GetValueAtPercentile(50) / 10000d}, {testHistogramW.GetValueAtPercentile(75) / 10000d}, {testHistogramW.GetValueAtPercentile(90) / 10000d}, {testHistogramW.GetValueAtPercentile(95) / 10000d}, {testHistogramW.GetValueAtPercentile(99) / 10000d}, {testHistogramW.GetValueAtPercentile(99.9) / 10000d}, {testHistogramW.TotalCount - failCtr}, {failCtr} ");

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Exception writing test results into scan_test_{DateTime.Now}:MM_dd_yy.csv,error: {ex.Message}");
            }
        }

        internal void ReadWriteMutlipleChannels()
        {
            testHistogramR = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            testHistogramW = new LongConcurrentHistogram(3, TimeStamp.Hours(1), 3);
            Type = "ReadWriteMultipleChannels";

            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Instance: {_settings.InstanceId}");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Starting {Type} test against Table: {_settings.TableName} for {_settings.RpcTestDurationMinutes} minutes using {_settings.RpcThreads} thread");
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} AppProfile ID: {_settings.AppProfileId}");

            List<Thread> threads = new List<Thread>();

            var runtime = Stopwatch.StartNew();

            for (var i = 0; i < _settings.RpcThreads; i++)
            {
                threads.Add(new Thread(() =>
                {
                    while (runtime.Elapsed.TotalMinutes < _settings.RpcTestDurationMinutes)
                    {
                        int rowNum = GetRandom(_settings.Records);
                        BigtableByteString rowKey = _settings.RowKeyPrefix + rowNum.ToString(_stringFormat);
#if DEBUG
                        Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Reading rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                        if (rowNum % 2 == 0)
                        {
                            var startReadTime = runtime.Elapsed;

                            try
                            {
                                _bigtableClient.ReadRow(_table, rowKey, new RowFilter { CellsPerColumnLimitFilter = 1 });
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine(
                                    $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startReadTime).TotalMilliseconds} milliseconds\n Failed to read rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endReadTime = runtime.Elapsed;
                                var responseTime = endReadTime - startReadTime;
                                testHistogramR.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                        else
                        {
                            var request = new MutateRowRequest
                            {
                                TableNameAsTableName = _table,
                                RowKey = rowKey.Value,
                                Mutations = { MutationsBuilder() }
                            };

#if DEBUG
                            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Inserting rowKey {rowKey.Value.ToStringUtf8()} ");
#endif
                            var startInsertTime = runtime.Elapsed;

                            try
                            {
                                _bigtableClient.MutateRow(request, CallSettings.FromCallTiming(CallTiming.FromTimeout(TimeSpan.FromMilliseconds(_settings.Timeout))));
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref failCtr);
                                Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} Elapsed {(runtime.Elapsed - startInsertTime).TotalMilliseconds} milliseconds\nFailed to insert rowKey {rowKey.Value.ToStringUtf8()}, error message: {ex.Message}");
                            }
                            finally
                            {
                                var endInsertTime = runtime.Elapsed;
                                var responseTime = endInsertTime - startInsertTime;
                                testHistogramW.RecordValue(responseTime.Ticks); // milliseconds * 10000
                            }
                        }
                    }
                }));
            }
            threads.ForEach(a => a.Start());
            threads.ForEach(a => a.Join());
        }

        private ChannelCredentials GetCredentials()
        {
            var path = Directory.GetCurrentDirectory();
            ChannelCredentials channelCredentials = GoogleCredential.FromComputeCredential().ToChannelCredentials();
            return channelCredentials;
        }


        private int GetRandom(int upperLimit)
        {
            try
            {
                int value;
                lock (lockObj)
                {
                    value = _rnd.Next(0, upperLimit);
                }
                return value;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        private Mutation[] MutationsBuilder()
        {
            Mutation[] mutations = new Mutation[_settings.Columns];
            //int rndIndex = GetRandom(RandSize);
            for (int i = 0; i < mutations.Length; i++)
            {
                mutations[i] = Mutations.SetCell(_settings.ColumnFamily, $"field{i}", Rand[GetRandom(RandSize)]);
            }
            return mutations;
        }
    }
}
