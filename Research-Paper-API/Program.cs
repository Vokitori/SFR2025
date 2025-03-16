using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro;
using Avro.Generic;
using Research_Paper_API.models;
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
                { ""name"": ""Keywords"", ""type"": { ""type"": ""array"", ""items"": ""string"" } }
            ]
        }";

        var avroSchema = (RecordSchema)Avro.Schema.Parse(schema);

        var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrl };

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using (var producer = new ProducerBuilder<string, GenericRecord>(config)
           .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
           .Build())
        {
            string json = File.ReadAllText("research_papers.json");
            List<Paper>? papers = JsonSerializer.Deserialize<List<Paper>>(json);

            foreach(Paper paper in papers)
            {
                if(paper.Id < 5)
                {
                    try
                    {
                        var researchPaper = new GenericRecord(avroSchema);
                        researchPaper.Add("Id", paper.Id);
                        researchPaper.Add("Name", paper.Name);
                        researchPaper.Add("Authors", paper.Authors);
                        researchPaper.Add("Keywords", paper.Keywords);

                        var message = new Message<string, GenericRecord>
                        {
                            Key = Guid.NewGuid().ToString(),
                            Value = researchPaper
                        };

                        var deliveryReport = await producer.ProduceAsync(topic, message);
                        Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Fehler beim Senden: {e.Error.Reason}");
                    }
                }
            }
        }
    }
}