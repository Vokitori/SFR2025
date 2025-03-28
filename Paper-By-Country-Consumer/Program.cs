using System;
using System.Threading;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Paper_By_Country_Consumer.NewFolder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Npgsql;

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

        var connectionString = "Host=localhost;Username=postgres;Password=admin;Database=sfr2025";

        using (var connection = new NpgsqlConnection(connectionString))
        {
            connection.Open();
            Console.WriteLine("connection open");
        }

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        Console.WriteLine("2");
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
            Console.WriteLine("3");

            consumer2.Subscribe(topic);
            Console.WriteLine("4");

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



                        //bool exists = collection.Find(p => p.Id == paper.Id).Any();
                        //if (exists)
                        //{
                        //    Console.WriteLine("❌ Paper already exists in MongoDB.");
                        //    continue;
                        //}
                        //collection.InsertOne(paper);
                        //Console.WriteLine("✅ Gespeichert in MongoDB.");
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
