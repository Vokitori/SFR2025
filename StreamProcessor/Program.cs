using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Confluent.Kafka.SyncOverAsync;
using Avro.IO.Parsing;

class KafkaConsumer
{
    public static async Task Main(string[] args)
    {
        string bootstrapServers = "localhost:29092";
        string schemaRegistryUrl = "http://localhost:8081";
        string topic = "research-paper";

        var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrl };

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "research-paper-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using (var consumer = new ConsumerBuilder<string, Paper>(config)
            .SetValueDeserializer(new AvroDeserializer<Paper>(schemaRegistry).AsSyncOverAsync())
            .Build())
        {
            consumer.Subscribe(topic);

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
                        var consumeResult = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed message with key {consumeResult.Message.Key}: {consumeResult.Message.Value}");
                        Paper paper = consumeResult.Message.Value;
                        papers.Add(paper);
                        Console.WriteLine($"Consumed Paper: Id={paper.Id}, Name={paper.Name}");
                        Console.WriteLine($"Authors: {string.Join(", ", paper.Authors)}");
                        Console.WriteLine($"Keywords: {string.Join(", ", paper.Keywords)}");
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
                consumer.Close();
            }
        }
    }
}
