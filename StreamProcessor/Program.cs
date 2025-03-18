using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Avro.Specific;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Avro;
using static Confluent.Kafka.ConfigPropertyNames;
using Streamiz.Kafka.Net.Stream;

class Program
{
    static void Main(string[] args)
    {
        var config = new StreamConfig<StringSerDes, SchemaAvroSerDes<Paper>>
        {
            ApplicationId = "research-paper-stream",
            BootstrapServers = "localhost:29092",
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
        };

        var builder = new StreamBuilder();


        builder.Stream<string, Paper>("RAW_PAPERS")
            .GroupBy<string, Paper>((key, paper) => paper.Keywords.FirstOrDefault() ?? "Unknown")
            .Aggregate(
                () => new PaperStats(),
                (key, paper, stats) =>
                {
                    stats.Keyword = key;
                    stats.PaperCount++;
                    stats.AuthorCount += paper.Authors.Count;
                    return stats;
                },
                InMemory.As<string, PaperStats>("aggregated-keywords")
        )
        .ToStream()
            .To("PROCESSED_PAPERS", Produced.With(new StringSerDes(), new AvroSerDes<PaperStats>()));

        var topology = builder.Build();
        var stream = new KafkaStream(topology, config);

        Console.CancelKeyPress += (sender, e) => stream.Dispose();
        stream.StartAsync().Wait();
    }
}

// Aggregated stats per keyword
public class PaperStats : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse(@"
    {
        ""type"": ""record"",
        ""name"": ""PaperStats"",
        ""fields"": [
            { ""name"": ""Keyword"", ""type"": ""string"" },
            { ""name"": ""PaperCount"", ""type"": ""int"" },
            { ""name"": ""AuthorCount"", ""type"": ""int"" }
        ]
    }");

    public Schema Schema => _SCHEMA;
    public string Keyword { get; set; }
    public int PaperCount { get; set; }
    public int AuthorCount { get; set; }

    public object Get(int fieldPos) =>
        fieldPos switch
        {
            0 => Keyword,
            1 => PaperCount,
            2 => AuthorCount,
            _ => throw new AvroRuntimeException("Unknown field position")
        };

    public void Put(int fieldPos, object value)
    {
        switch (fieldPos)
        {
            case 0: Keyword = (string)value; break;
            case 1: PaperCount = (int)value; break;
            case 2: AuthorCount = (int)value; break;
            default: throw new AvroRuntimeException("Unknown field position");
        }
    }
}



//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading;
//using Confluent.Kafka;
//using Confluent.Kafka.SyncOverAsync;
//using Confluent.SchemaRegistry;
//using Confluent.SchemaRegistry.Serdes;
//using Avro.Generic;

//class Program
//{
//    static void Main(string[] args)
//    {
//        string bootstrapServers = "localhost:29092";
//        string inputTopic = "research-paper";
//        string outputTopic = "aggregated_papers";
//        string groupId = "paper-processor-group";

//        var config = new ConsumerConfig
//        {
//            BootstrapServers = bootstrapServers,
//            GroupId = groupId,
//            AutoOffsetReset = AutoOffsetReset.Earliest
//        };

//        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };

//        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
//        using var consumer = new ConsumerBuilder<string, Paper>(config)
//            .SetValueDeserializer(new AvroDeserializer<Paper>(schemaRegistry).AsSyncOverAsync())
//            .Build();

//        var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

//        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

//        consumer.Subscribe(inputTopic);

//        CancellationTokenSource cts = new CancellationTokenSource();
//        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

//        try
//        {
//            while (!cts.Token.IsCancellationRequested)
//            {
//                try
//                {
//                    var consumeResult = consumer.Consume(cts.Token);
//                    Paper paper = consumeResult.Message.Value;

//                    // ✅ Process: Group papers by first keyword
//                    string keyword = paper.Keywords.FirstOrDefault() ?? "Unknown";

//                    // ✅ Produce new message to aggregated topic
//                    producer.Produce(outputTopic, new Message<string, string>
//                    {
//                        Key = keyword,
//                        Value = $"Paper: {paper.Name} by {string.Join(", ", paper.Authors)}"
//                    });

//                    Console.WriteLine($"Processed Paper: {paper.Name} -> {keyword}");
//                }
//                catch (ConsumeException e)
//                {
//                    Console.WriteLine($"Consume error: {e.Error.Reason}");
//                }
//            }
//        }
//        catch (OperationCanceledException) { }
//        finally
//        {
//            consumer.Close();
//        }
//    }
//}
