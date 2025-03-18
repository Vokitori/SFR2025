import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams;
import org.apache.kafka.streams.kstream;
import java.util.*;
import java.util.stream.Collectors;

public class PaperKafkaStream {
    private static final String TOPIC_PAPER_RAW = "paper_raw";
    private static final String TOPIC_AGGREGATED_PAPERS = "aggregated_papers";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "paper-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Paper> paperStream = builder.stream(TOPIC_PAPER_RAW, Consumed.with(Serdes.String(), new PaperSerde()));

        KTable<String, Long> keywordCounts = paperStream
            .flatMap((key, paper) -> paper.getKeywords().stream()
                .map(keyword -> KeyValue.pair(keyword, paper))
                .collect(Collectors.toList()))
            .groupByKey(Grouped.with(Serdes.String(), new PaperSerde()))
            .count(Materialized.as("keyword-counts-store"));

        keywordCounts.toStream().to(TOPIC_AGGREGATED_PAPERS, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}