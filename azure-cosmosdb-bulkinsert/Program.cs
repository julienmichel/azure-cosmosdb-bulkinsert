using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace azure_cosmosdb_bulkinsert
{
    class Program
    {
        private static string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static string ContainerName = ConfigurationManager.AppSettings["ContainerName"];
        private const int ConcurrentWorkers = 100;
        private const int ConcurrentDocuments = 1;

        static async Task Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            CosmosClient cosmosClient = new CosmosClient(ConnectionString, new CosmosClientOptions()
            {
                AllowBulkExecution = true,
                ConnectionMode = ConnectionMode.Direct,
                MaxRequestsPerTcpConnection = -1,
                MaxTcpConnectionsPerEndpoint = -1,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryAttemptsOnRateLimitedRequests = 999,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromHours(1),
            });

            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseName);

            Console.WriteLine("Creation of a 50000 RU/s container, press any key to continue.");
            Console.ReadKey();

            await database.DefineContainer(ContainerName, "/pk")
                    .WithIndexingPolicy()
                        .WithIndexingMode(IndexingMode.Consistent)
                        .WithIncludedPaths()
                            .Attach()
                        .WithExcludedPaths()
                            .Path("/*")
                            .Attach()
                    .Attach()
                .CreateIfNotExistsAsync(50000);

            string filePath = @ConfigurationManager.AppSettings["FilePath"];

            Stopwatch swcosmos = new Stopwatch();
            swcosmos.Start();

            IReadOnlyCollection<PricePaidData> itemsToInsert = GetItemsToInsert(filePath);
            Console.WriteLine("Collection of " + itemsToInsert.Count.ToString() + " created");

            Container container = cosmosClient.GetContainer(DatabaseName, ContainerName);

            int itemsCreated = 0;
            List<Task> tasks = new List<Task>(itemsToInsert.Count);

            foreach (PricePaidData item in itemsToInsert)
            {
                tasks.Add(
                    container.CreateItemAsync<PricePaidData>(item, new PartitionKey(item.pk))
                    .ContinueWith((Task<ItemResponse<PricePaidData>> task) =>
                    {
                        if (!task.IsCompletedSuccessfully)
                        {
                            AggregateException innerExceptions = task.Exception.Flatten();
                            CosmosException cosmosException = innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) as CosmosException;
                            Console.WriteLine($"Item {item.Transaction_unique_identifieroperty} failed with status code {cosmosException.StatusCode}");
                        }
                    }));
            }

            await Task.WhenAll(tasks);
            itemsCreated += tasks.Count(task => task.IsCompletedSuccessfully);

            Console.WriteLine("CosmosDB ingestion time : " + (swcosmos.ElapsedMilliseconds / 1000).ToString() + " seconds");
            swcosmos.Stop();

            Console.WriteLine("Total Time writing to CosmosDB : " + (sw.ElapsedMilliseconds / 1000).ToString() + " seconds");
            sw.Stop();

            Console.WriteLine($"Executed process with {tasks.Count} worker threads");
            Console.WriteLine($"Inserted {itemsCreated} items.");
        }

        private static async Task CreateItemsConcurrentlyAsync(CosmosClient client, IReadOnlyCollection<PricePaidData> itemsToInsert)
        {
            // Create concurrent workers that will insert items for 30 seconds
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(30000);
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            Container container = client.GetContainer(DatabaseName, ContainerName);
            List<Task> workerTasks = new List<Task>();

            Console.WriteLine($"Initiating process with {ConcurrentWorkers} worker threads writing groups of {ConcurrentDocuments} items");

            workerTasks.Add(CreateItemsAsync(container, cancellationToken, itemsToInsert));

            Console.WriteLine($"Executed process with {workerTasks.Count} worker threads");
            await Task.WhenAll(workerTasks);
        }

        private static async Task<int> CreateItemsAsync(Container container, CancellationToken cancellationToken, IReadOnlyCollection<PricePaidData> itemsToInsert)
        {
            int itemsCreated = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                List<Task> tasks = new List<Task>(ConcurrentDocuments);
                foreach (PricePaidData item in itemsToInsert)
                {
                    tasks.Add(
                        container.CreateItemAsync<PricePaidData>(item, new PartitionKey(item.pk))
                        .ContinueWith((Task<ItemResponse<PricePaidData>> task) =>
                        {
                            if (!task.IsCompletedSuccessfully)
                            {
                                AggregateException innerExceptions = task.Exception.Flatten();
                                CosmosException cosmosException = innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) as CosmosException;
                                Console.WriteLine($"Item {item.Transaction_unique_identifieroperty} failed with status code {cosmosException.StatusCode}");
                            }
                        }));
                }

                await Task.WhenAll(tasks);

                itemsCreated += tasks.Count(task => task.IsCompletedSuccessfully);
            }

            return itemsCreated;
        }

        static private IReadOnlyCollection<PricePaidData> GetItemsToInsert(string filePath)
        {
            Console.WriteLine("Reading the file to generate a collection of Items");

            List<PricePaidData> lstPricedata = new List<PricePaidData>();
            using (FileStream fs = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (BufferedStream bs = new BufferedStream(fs))
                {
                    using (StreamReader sr = new StreamReader(bs))
                    {
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            string[] pricedataArray = line.Split(',');
                            PricePaidData ppd = new PricePaidData()
                            {
                                Transaction_unique_identifieroperty = pricedataArray[0],
                                Price = pricedataArray[1].Replace("\"", ""),
                                Date_of_Transfer = pricedataArray[2].Replace("\"", ""),
                                Postcode = pricedataArray[3].Replace("\"", ""),
                                PropertyType = pricedataArray[4].Replace("\"", ""),
                                isNew = pricedataArray[5].Replace("\"", ""),
                                Duration = pricedataArray[6].Replace("\"", ""),
                                PAON = pricedataArray[7].Replace("\"", ""),
                                SAON = pricedataArray[8].Replace("\"", ""),
                                Street = pricedataArray[9].Replace("\"", ""),
                                Locality = pricedataArray[10].Replace("\"", ""),
                                Town_City = pricedataArray[11].Replace("\"", ""),
                                District = pricedataArray[12].Replace("\"", ""),
                                County = pricedataArray[13].Replace("\"", ""),
                                PPD_Category = pricedataArray[14].Replace("\"", ""),
                                Record_Status = pricedataArray[15].Replace("\"", ""),
                                pk = pricedataArray[0]

                            };
                            lstPricedata.Add(ppd);
                        }
                    }
                }
            }
            return lstPricedata;
        }

    }
}
