using System;
using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.Configuration;
using Mono.Options;

namespace Google.Cloud.Bigtable.V2.ReadWriteTests
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Load configurations

            var builder = new ConfigurationBuilder().
                SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", false, true);

            IConfigurationRoot configuration = builder.Build();

            ReadWriteTestsSetting settings = new ReadWriteTestsSetting();
            configuration.Bind(settings);
            #endregion

            #region Commandline Parser

            var show_help = false;

            var parser = new OptionSet()
            {
                "Options:",
                {"i|instance=", "Bigtable InstanceID name", v => settings.InstanceId = v},
                {"o|timeout=", "MutateRow call timeout setting, default 20000 milliseconds", v => settings.Timeout = Convert.ToInt32(v)},
                {"T|TableName=", "TableName to use", v => settings.TableName = v },
                {"t|test=", "Single RPC Test type:\n" +
                               "1 for ReadOnly test on main thread againgst Bigtable server\n" +
                               "2 for ReadOnly test using mutliple threads against Bigtable server\n" +
                               "3 for WriteOnly test on main thread against Bigtable server\n" +
                               "4 for WriteOnly test using mutliple threads against Bigtable server\n" +
                               "5 for ReadWrite test using multiple threads against Bigtable server\n" +
                               "6 for ReadOnly test on main thread against mock server\n" +
                               "7 for ReadOnly test using mutliple threads against mock server\n" +
                               "8 for WriteOnly test on main thread against mock server\n" +
                               "9 for WriteOnly test using mutliple threads against mock server\n" +
                               "10 for ReadWrite test using multiple threads against mock server\n" +
                               "11 for C# ReadOnly test without invoking RPC\n" +
                               "12 for C# WriteOnly test without invoking RPC\n" +
                               "13 for MutationsBuilderTest\n" +
                               "14 for RandomPerformanceTest\n" +
                               "15 for ReadWrite test using multiple threads via multiple channels against Bigtable server\n" +
                               "16 for WriteOnly test using multiple threads via multiple channels against Bigtable server\n" +
                               "17 for ReadOnly test using multiple threads via multiple channels against Bigtable server\n" +
                               "18 for LoadTest\n" +
                               "19 for Latesncy Test\n" +
                               "  default t = 1",
                    v =>
                    {
                        if (Convert.ToInt32(v) < 1 || Convert.ToInt32(v) > 19)
                        {
                            show_help = v != null;
                            throw new OptionException("Wron input for Single RPC test type", "-t");
                        }
                        settings.TestType = Convert.ToInt32(v);
                    }
                },
                //{ "r|rows=", "Number of rows to load", v => settings.Records = Convert.ToInt32(v)},
                {"l|load=", "Number of load threads", v => settings.LoadThreads = Convert.ToInt32(v)},
                {"m|minutes=", "RPC test duration minutes", v => settings.RpcTestDurationMinutes = Convert.ToInt32(v)},
                {"e|rpc=", "Number of RPC threads", v => settings.RpcThreads = Convert.ToInt32(v)},
                {"H|Help", "Show this message and exit", v => show_help = v != null}
            };

            try
            {
                parser.Parse(args);
            }
            catch (OptionException ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine("Try `-H|--Help' for more information.");
                parser.WriteOptionDescriptions(Console.Out);
                return;
            }

            if (show_help)
            {
                parser.WriteOptionDescriptions(Console.Out);
                return;
            }
            #endregion

            ReadWriteTests runner = new ReadWriteTests(settings);
            runner.FillRandomData();
            
            var stopWatch = Stopwatch.StartNew();
            switch (settings.TestType)
            {
                case 1:
                case 6:
                    {
                        runner.ReadOnlyTest();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 2:
                case 7:
                    {
                        runner.ReadOnlyTestMultipleThreads();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 3:
                case 8:
                    {
                        runner.WriteOnlyTest();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 4:
                case 9:
                    {
                        runner.WriteOnlyTestMultipleThreads();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 5:
                case 10:
                    {
                        runner.ReadWriteMutlipleThreads();
                        stopWatch.Stop();

                        runner.WriteCsvToConsoleReadWrite(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsvReadWrite(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 11:
                    {
                        runner.ReadOnlyTestCSharp();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 12:
                    {
                        runner.WriteOnlyTestCSharp();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 13:
                    {
                        stopWatch.Stop();
                        runner.MutationsBuilderTest();
                        runner.MutationsBuilderTestMultiThread();
                        break;
                    }
                case 14:
                    {
                        stopWatch.Stop();
                        runner.RandomPerformanceTest();
                        runner.RandomPerformanceTestMultiThread();
                        break;
                    }
                case 15:
                    {
                        runner.ReadWriteMutlipleChannels();
                        stopWatch.Stop();

                        runner.WriteCsvToConsoleReadWrite(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsvReadWrite(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 16:
                    {
                        runner.WriteOnlyTestMultipleChannels();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 17:
                    {
                        runner.ReadOnlyTestMultipleChannels();
                        stopWatch.Stop();

                        runner.WriteCsvToConsole(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsv(settings.RpcTestDurationMinutes * 60d);
                        break;
                    }
                case 18:
                    {
                        settings.TableName = runner.GetRandomTableName();
                        runner.SetTableName();
                        runner.CreateTable();
                        var startTest = stopWatch.Elapsed;
                        runner.LoadTest();
                        stopWatch.Stop();
                        var endTest = stopWatch.Elapsed;
                        runner.DeleteTable();
                        var totalTime = (endTest - startTest).TotalSeconds;

                        Console.WriteLine($"Loaded {settings.Records} in {totalTime} seconds or {settings.Records / totalTime} rows per second ");
                        runner.WriteCsvToConsole(totalTime);
                        runner.WriteCsv(totalTime);
                        break;
                    }
                case 19:
                    {
                        settings.TableName = runner.GetRandomTableName();
                        runner.SetTableName();
                        runner.CreateTable();
                        var startTest = stopWatch.Elapsed;
                        runner.LoadTest();
                        stopWatch.Stop();
                        var endTest = stopWatch.Elapsed;
                        var totalTime = (endTest - startTest).TotalSeconds;

                        Console.WriteLine($"Loaded {settings.Records} in {totalTime:N0} seconds or {settings.Records / totalTime:N0} rows per second ");
                        runner.WriteCsvToConsole(totalTime);
                        runner.WriteCsv(totalTime);

                        runner.ReadWriteMutlipleChannels();
                        runner.WriteCsvToConsoleReadWrite(settings.RpcTestDurationMinutes * 60d);
                        runner.WriteCsvReadWrite(settings.RpcTestDurationMinutes * 60d);

                        runner.DeleteTable();
                        break;
                    }
            }
#if DEBUG
            Console.ReadKey();
#endif
        }
    }
}
