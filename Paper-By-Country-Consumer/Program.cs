using System;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

class Program
{
   
    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:29092",
            GroupId = "processed-papers-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081"
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, long>(config)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(/*Deserializers.Int64*/new AvroDeserializer<long>(schemaRegistry).AsSyncOverAsync())
            .Build();

        consumer.Subscribe("PROCESSED_PAPERS");

        Console.WriteLine("Consuming messages from PROCESSED_PAPERS...");

        try
        {
            while (true)
            {
                var result = consumer.Consume(CancellationToken.None);
                Console.WriteLine($"Country: {result.Key}, Paper Count: {result.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
}
