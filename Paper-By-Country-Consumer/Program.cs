using System;
using System.Threading;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using MongoDB.Driver;


class Program
{
   
    static void Main(string[] args)
    {
        //var config = new ConsumerConfig
        //{
        //    BootstrapServers = "localhost:29092",
        //    GroupId = "processed-papers-consumer",
        //    AutoOffsetReset = AutoOffsetReset.Earliest
        //};

        var config2 = new ConsumerConfig
        {
            BootstrapServers = "localhost:29092",
            GroupId ="research-paper-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081"
        };

        const string connectionUri = "mongodb+srv://sfr2025:X0kUfHUdrf8ohI0i@papers.h8l9c64.mongodb.net/?retryWrites=true&w=majority&appName=Papers";
        var settings = MongoClientSettings.FromConnectionString(connectionUri);
        settings.ServerApi = new ServerApi(ServerApiVersion.V1);
        var client = new MongoClient(settings);
        var database = client.GetDatabase("papers");
        var collection = database.GetCollection<Paper>("Papers");

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        
        //using var consumer = new ConsumerBuilder<string, long>(config)
        //    .SetKeyDeserializer(Deserializers.Utf8)
        //    .SetValueDeserializer(new AvroDeserializer<long>(schemaRegistry).AsSyncOverAsync())
        //    .Build();

        

        //consumer.Subscribe("PROCESSED_PAPERS");

        //Console.WriteLine("Consuming messages from PROCESSED_PAPERS...");

        //try
        //{
        //    while (true)
        //    {
        //        var result = consumer.Consume(CancellationToken.None);
        //        Console.WriteLine($"Country: {result.Key}, Paper Count: {result.Value}");
        //    }
        //}
        //catch (OperationCanceledException)
        //{
        //    consumer.Close();
        //}




        string topic = "research-paper";
        using (var consumer2 = new ConsumerBuilder<string, Paper>(config2)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new AvroDeserializer<Paper>(schemaRegistry).AsSyncOverAsync())
            .Build())
        {

            consumer2.Subscribe(topic);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            List<Paper> papers = new List<Paper>();
            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer2.Consume(cts.Token);
                        Console.WriteLine($"Consumed message with key {consumeResult.Message.Key}: {consumeResult.Message.Value}");
                        Paper paper = (consumeResult.Message.Value);
                        papers.Add(paper);
                        Console.WriteLine($"Consumed Paper: Id={paper.Id}, Name={paper.Name}");
                        Console.WriteLine($"Authors: {string.Join(", ", paper.Authors)}");
                        Console.WriteLine($"Keywords: {string.Join(", ", paper.Keywords)}");



                        bool exists = collection.Find(p => p.Id == paper.Id).Any();
                        if (exists)
                        {
                            Console.WriteLine("❌ Paper already exists in MongoDB.");
                            continue;
                        }
                        collection.InsertOne(paper);
                        Console.WriteLine("✅ Gespeichert in MongoDB.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consuming operation was canceled");
            }
            finally
            {
                consumer2.Close();
            }
        }
    }
}
