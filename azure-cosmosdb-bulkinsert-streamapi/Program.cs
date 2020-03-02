using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace azure_cosmosdb_bulkinsert_streamapi
{
    internal class Program
    {
        private static string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static string ContainerName = ConfigurationManager.AppSettings["ContainerName"];

        private const int BatchSize = 10000;

        private static ManualResetEventSlim ingestionProcessingResetEvent = new ManualResetEventSlim();

        private static int documentsIngested;

        private static int Main(string[] args)
        {
            using (var cancellationTokenSource = new CancellationTokenSource())
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                // Stop ingestion after 30 seconds for testing purposes
                CancellationToken cancellationToken = cancellationTokenSource.Token;

                Console.WriteLine("Starting bulk ingestion...");

                // Offload bulk ingestion to thread pool thread
                DoBulkIngestionAsync(cancellationToken);

                while (!ingestionProcessingResetEvent.IsSet)
                {
                    Console.WriteLine("Press Q to quit...");
                    ConsoleKeyInfo key = Console.ReadKey();

                    if (key.Key == ConsoleKey.Q)
                    {
                        cancellationTokenSource.Cancel();

                        Console.WriteLine("Aborting bulk ingestion...");
                        ingestionProcessingResetEvent.Wait();
                    }
                }

                Console.WriteLine("Total Time writing to CosmosDB : " + (sw.ElapsedMilliseconds / 1000).ToString() + " seconds");
                sw.Stop();

                Console.WriteLine($"Inserted {documentsIngested} items.");

                Console.WriteLine("Finished - press any key to quit program...");
                Console.ReadKey();

                return 0;
            }
        }

        private static async void DoBulkIngestionAsync(CancellationToken cancellationToken)
        {
            // Ensure ingestion runs on background thread...
            await Task.Yield();

            var watch = Stopwatch.StartNew();

            try
            {
                var clientOptions = new CosmosClientOptions()
                {
                    AllowBulkExecution = true,
                    ConnectionMode = ConnectionMode.Direct,
                    MaxRequestsPerTcpConnection = -1,
                    MaxTcpConnectionsPerEndpoint = -1,
                    ConsistencyLevel = ConsistencyLevel.Eventual,
                    MaxRetryAttemptsOnRateLimitedRequests = 999,
                    MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromHours(1),
                };

                using (var cosmosClient = new CosmosClient(ConnectionString, clientOptions))
                {
                    Database database =
                        await cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseName).IgnoreCapturedContext();

                    Console.WriteLine("Creating or Retrieving a container of 50000 RU/s container...");

                    ContainerResponse containerResponse = await database.DefineContainer(ContainerName, "/pk")
                            .WithIndexingPolicy()
                                .WithIndexingMode(IndexingMode.Consistent)
                                .WithIncludedPaths()
                                    .Path("/pk/?")
                                    .Attach()
                                .WithExcludedPaths()
                                    .Path("/*")
                                    .Attach()
                            .Attach()
                        .CreateIfNotExistsAsync(50000).IgnoreCapturedContext();

                    string filePath = @ConfigurationManager.AppSettings["FilePath"];

                    Stopwatch swcosmos = new Stopwatch();
                    swcosmos.Start();

                    Container container = containerResponse.Container;

                    // Determines how many parallel threads try to ingest batches of BatchSize documents
                    // Usually roughly the number of physcial cores is a good starting point
                    // Increase this if your ingestion client isn't located in the same Azure region as the
                    // CosmosDB account and your client is powerful enough to handle the additional CPU load and
                    // memory pressure
                    using (var concurrencyThrottle = new SemaphoreSlim(Environment.ProcessorCount * 2))
                    using (FileStream fs = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                    using (var bs = new BufferedStream(fs))
                    using (var sr = new StreamReader(bs))
                    {
                        var eofReached = false;

                        var batchIngestionTasks = new List<Task>();

                        while (!eofReached && !cancellationToken.IsCancellationRequested)
                        {
                            // Ensure max concurrency limit to protect client form getting overloaded or running
                            // out of memory
                            await concurrencyThrottle.WaitAsync().IgnoreCapturedContext();

                            // retrieve the next BatchSize items from the
                            IReadOnlyCollection<PricePaidData> items =
                                await GetItemsForNextBatchAsync(sr).IgnoreCapturedContext();

                            if (items.Count == 0)
                                break;
                            else if (items.Count < BatchSize)
                                eofReached = true;

                            // offload actual ingestion to another thread pool thread
                            batchIngestionTasks.Add(
                                IngestBatchAsync(container, items, concurrencyThrottle, cancellationToken));
                        }

                        await Task.WhenAll(batchIngestionTasks).IgnoreCapturedContext();
                    }
                }
            }
            catch (Exception unhandledException)
            {
                Console.WriteLine("UNHANDLED EXCEPTION: {0}", unhandledException);
            }
            finally
            {
                watch.Stop();
                Console.WriteLine(
                    "INEGSTION COMPLETED in {0} ms- Press any key to proceed.",
                    watch.Elapsed.TotalMilliseconds.ToString("#,###,###,###,##0.00000", CultureInfo.InvariantCulture));

                ingestionProcessingResetEvent.Set();
            }
        }

        private static async Task IngestBatchAsync(Container container, IReadOnlyCollection<PricePaidData> items, SemaphoreSlim concurrencyThrottle, CancellationToken cancellationToken)
        {
            try
            {
                var ingestionTasks = new List<Task>(BatchSize);

                foreach (PricePaidData item in items)
                {
                    ingestionTasks.Add(
                        container.UpsertItemAsync<PricePaidData>(
                            item,
                            new PartitionKey(item.pk),
                            requestOptions: null,
                            cancellationToken)
                        .ContinueWith(
                            (Task<ItemResponse<PricePaidData>> task) =>
                            {
                                if (!task.IsCompletedSuccessfully)
                                {
                                    AggregateException innerExceptions = task.Exception.Flatten();
                                    var cosmosException =
                                        innerExceptions.InnerExceptions.FirstOrDefault(
                                            innerEx => innerEx is CosmosException) as CosmosException;
                                    Console.WriteLine(
                                        $"Item {item.Transaction_unique_identifieroperty} failed with " +
                                        $"status code {cosmosException.StatusCode}");
                                }
                                else
                                {
                                    Interlocked.Increment(ref documentsIngested);
                                }
                            },
                            TaskScheduler.Default));
                }

                await Task.WhenAll(ingestionTasks).IgnoreCapturedContext();

                Console.WriteLine(
                    "STATUS: {0} documents ingested successfully",
                    documentsIngested.ToString("#,###,###,##0", CultureInfo.InvariantCulture));
            }
            finally
            {
                concurrencyThrottle.Release();
            }
        }

        private static async Task<IReadOnlyCollection<PricePaidData>> GetItemsForNextBatchAsync(StreamReader sr)
        {
            var lstPricedata = new List<PricePaidData>();
            string line;
            while ((line = await sr.ReadLineAsync().IgnoreCapturedContext()) != null &&
                lstPricedata.Count < BatchSize)
            {
                var pricedataArray = line.Split(',');
                var ppd = new PricePaidData()
                {
                    Transaction_unique_identifieroperty = pricedataArray[0],
                    Price = pricedataArray[1].Replace("\"", "", StringComparison.Ordinal),
                    Date_of_Transfer = pricedataArray[2].Replace("\"", "", StringComparison.Ordinal),
                    Postcode = pricedataArray[3].Replace("\"", "", StringComparison.Ordinal),
                    PropertyType = pricedataArray[4].Replace("\"", "", StringComparison.Ordinal),
                    isNew = pricedataArray[5].Replace("\"", "", StringComparison.Ordinal),
                    Duration = pricedataArray[6].Replace("\"", "", StringComparison.Ordinal),
                    PAON = pricedataArray[7].Replace("\"", "", StringComparison.Ordinal),
                    SAON = pricedataArray[8].Replace("\"", "", StringComparison.Ordinal),
                    Street = pricedataArray[9].Replace("\"", "", StringComparison.Ordinal),
                    Locality = pricedataArray[10].Replace("\"", "", StringComparison.Ordinal),
                    Town_City = pricedataArray[11].Replace("\"", "", StringComparison.Ordinal),
                    District = pricedataArray[12].Replace("\"", "", StringComparison.Ordinal),
                    County = pricedataArray[13].Replace("\"", "", StringComparison.Ordinal),
                    PPD_Category = pricedataArray[14].Replace("\"", "", StringComparison.Ordinal),
                    Record_Status = pricedataArray[15].Replace("\"", "", StringComparison.Ordinal),
                    pk = pricedataArray[0]
                };

                lstPricedata.Add(ppd);
            }

            return lstPricedata;
        }
    }
}