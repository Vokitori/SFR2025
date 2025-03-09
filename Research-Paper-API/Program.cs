using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Research_Paper_API.models;

class KafkaProducer
{
    public static async Task Main(string[] args)
    {
        string bootstrapServers = "localhost:39092"; // Kafka Broker Adresse
        string topic = "research-paper"; // Topic-Name

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        };

        using (var producer = new ProducerBuilder<int, string>(config).Build())
        {
            string json = File.ReadAllText("research_papers.json");
            List<Paper>? papers = JsonSerializer.Deserialize<List<Paper>>(json);
            for (int i = 0; i <= 10; i++)
            {
                try
                {
                    var deliveryReport = await producer.ProduceAsync(topic, new Message<int, string> { Key = papers[i].Id, Value = JsonSerializer.Serialize(papers[i]) });
                    Console.WriteLine($"Gesendet: {JsonSerializer.Serialize(papers[i])} an Partition {deliveryReport.Partition}, Offset {deliveryReport.Offset}");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Fehler beim Senden: {e.Error.Reason}");
                }
            }
        }
    }
}