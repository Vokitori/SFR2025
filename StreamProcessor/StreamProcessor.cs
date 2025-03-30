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
using Streamiz.Kafka.Net.Processors;

/// <summary>
/// 
/// Topic PROCESSED_PAPERS needs to be added to Kafka before streams can be applied
/// 
/// fail: Streamiz.Kafka.Net.Kafka.Internal.RecordCollector[0]
//stream-task[1|0] Error encountered sending record to topic PROCESSED_PAPERS for task 1-0 due to:
//stream-task[1|0] Error Code : UnknownTopicOrPart
//stream-task[1|0] Message : Broker: Unknown topic or partition
//stream-task[1|0] Exception handler choose to FAIL the processing, no more records would be sent.fail: Streamiz.Kafka.Net.Kafka.Internal.RecordCollector[0]
//stream-task[1|0] Error encountered sending record to topic PROCESSED_PAPERS for task 1-0 due to:
//stream-task[1|0] Error Code : UnknownTopicOrPart
//stream-task[1|0] Message : Broker: Unknown topic or partition
//stream-task[1|0] Exception handler choose to FAIL the processing, no more records would be sent.
/// </summary>

class StreamProcessor
{
    static void Main(string[] args)
    {
        var config = new StreamConfig<StringSerDes, SchemaAvroSerDes<Paper>>
        {
            ApplicationId = "research-paper-stream",
            BootstrapServers = "localhost:29092",
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
            SchemaRegistryUrl = "http://localhost:8081"
        };

        var builder = new StreamBuilder();

        builder.Stream<string, Paper>("research-paper")
            .GroupBy(new CountryMapper())
            .Count(InMemory.As<string, long>("paper-count-by-country"))
            .ToStream()
            .To("PROCESSED_PAPERS", new StringSerDes(), new SchemaAvroSerDes<long>()); ;


        var topology = builder.Build();
        var stream = new KafkaStream(topology, config);

        Console.CancelKeyPress += (sender, e) => stream.Dispose();
        stream.StartAsync().Wait();
    }

}

public class CountryMapper : IKeyValueMapper<string, Paper, string>
{
    public string Apply(string key, Paper paper, IRecordContext context)
    {
        return paper.CountryOfPublication ?? "Unknown";
    }
}

