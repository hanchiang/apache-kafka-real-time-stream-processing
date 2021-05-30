package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloStreams {
    private static final Logger logger = LogManager.getLogger(HelloProducer.class);

    public static void main(String[] main) {
        // Create config
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        /**
         * The reason for using a serde is because a typical kafka streams application will be
         * reading and writing data
         */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Open a stream to a source topic, write the processing logic
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<Integer, String> kStream = streamsBuilder.stream(AppConfigs.topicName);
        kStream.foreach((k, v) -> System.out.println("Key = " + k + " Value  = " + v));
        // kStream.peek((k, v) -> System.out.println("Key = " + k + " Value  = " + v));

        // Create a topology(core of the kafka streams computational logic)
        // The logic of streamsBuilder.stream and kStream.foreach are bundled in the topology object
        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        logger.info("Starting stream");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            streams.close();
        }));
    }
}
