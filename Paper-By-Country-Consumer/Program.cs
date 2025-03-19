using System;
using System.Threading;
using Confluent.Kafka;

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

        using var consumer = new ConsumerBuilder<string, long>(config)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(Deserializers.Int64)
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
