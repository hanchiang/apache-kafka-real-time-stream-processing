package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PosSimulator {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        // Set up kafka configuration
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Create producer and threads
        KafkaProducer<String, PosInvoice> producer = new KafkaProducer(props);
        ExecutorService executor = Executors.newFixedThreadPool(AppConfigs.numThreads);
        List<PosRunnable> runnableProducers = new ArrayList<>();

        for (int i = 0; i < AppConfigs.numThreads; i++) {
            logger.info("Starting thread " + i);
            PosRunnable runnableProducer = new PosRunnable(producer, AppConfigs.topicName, AppConfigs.delayBetweenMessage);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (PosRunnable runnableProducer : runnableProducers) {
                    runnableProducer.stop();
                }
                executor.shutdown();
                logger.info("Closing executor");

                try {
                    executor.awaitTermination(2 * AppConfigs.delayBetweenMessage, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error(e.getStackTrace());
                    throw new RuntimeException(e);
                }
            }
        });
    }

}
