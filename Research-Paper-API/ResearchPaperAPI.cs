using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro;
using Avro.Generic;
using System.Text.Json;

class KafkaProducer
{
    public static async Task Main(string[] args)
    {
        string bootstrapServers = "localhost:29092";
        string schemaRegistryUrl = "http://localhost:8081";
        string topic = "research-paper";

        var schema = @"
        {
            ""type"": ""record"",
            ""name"": ""ResearchPaper"",
            ""fields"": [
                { ""name"": ""Id"", ""type"": ""int"" },
                { ""name"": ""Name"", ""type"": ""string"" },
                { ""name"": ""Authors"", ""type"": { ""type"": ""array"", ""items"": ""string"" } },
                { ""name"": ""Keywords"", ""type"": { ""type"": ""array"", ""items"": ""string"" } },
                { ""name"": ""CountryOfPublication"", ""type"": ""string"" }

            ]
        }";

        var avroSchema = (RecordSchema)Avro.Schema.Parse(schema);

        var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrl };

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

        using (var producer = new ProducerBuilder<string, Paper>(config)
                   .SetValueSerializer(new AvroSerializer<Paper>(schemaRegistry))
                   .Build())
        {
            string json = File.ReadAllText("research_papers_with_country.json");
            List<Paper>? papers = JsonSerializer.Deserialize<List<Paper>>(json);

            
            for (int i = 0; i < papers.Count; i++)
            {
                var paper = papers[i];

                try
                {
                    var message = new Message<string, Paper>
                    {
                        Key = Guid.NewGuid().ToString(),
                        Value = paper
                    };

                    var deliveryReport = await producer.ProduceAsync(topic, message);
                    Console.WriteLine($"Produced message {i + 1} to {deliveryReport.TopicPartitionOffset}");
                }
                catch (ProduceException<string, Paper> e)
                {
                    Console.WriteLine($"Fehler beim Senden: {e.Error.Reason}");
                }

                // Nach jedem 5. Paper (außer beim letzten) warten
                if ((i + 1) % 5 == 0 && i + 1 < papers.Count)
                {
                    Console.WriteLine("Press any key to send next 5 papers...");
                    Console.ReadKey();
                }
            }
        }
    }
}