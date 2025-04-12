using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Paper_By_Country_Consumer;

class WriteToDBConsumer
{
   
    static void Main(string[] args)
    {
       var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:29092",
            GroupId ="research-paper-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081"
        };

        DbHelper.EnsureTableExists();

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        string topicResearchPaper = "research-paper";
        using (var researchPaperConsumer = new ConsumerBuilder<string, Paper>(consumerConfig)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(new AvroDeserializer<Paper>(schemaRegistry).AsSyncOverAsync())
            .Build())
        {

            researchPaperConsumer.Subscribe(topicResearchPaper);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = researchPaperConsumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed message with key {consumeResult.Message.Key}: {consumeResult.Message.Value}");
                        Paper paper = (consumeResult.Message.Value);                        
                        Console.WriteLine($"Consumed Paper: Id={paper.Id}, Name={paper.Name}");
                        Console.WriteLine($"Authors: {string.Join(", ", paper.Authors)}");
                        Console.WriteLine($"Keywords: {string.Join(", ", paper.Keywords)}");
                        Console.WriteLine($"---------------------------------------------------\n");

                        DbHelper.SavePaper(paper);

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
                researchPaperConsumer.Close();
            }

        }
    }
}
