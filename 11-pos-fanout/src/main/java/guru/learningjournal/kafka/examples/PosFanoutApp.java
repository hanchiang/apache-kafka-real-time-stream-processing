package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/**
 * XYZ is a Home Furniture and Kitchen utensils retailer. They have twenty retail stores spread all over the country. XYZ
 * management decided to transform themselves into a real-time data-driven organization. As a first step towards that goal, they
 * started sending their invoices to a Kafka cluster in real-time. The POS machines in all their stores are now sending invoices to a
 * Kafka topic – POS. As a next step, they want to create the following automated services.
 * 1. Shipment Service
 * 2. Loyalty Management Service
 * 3. Trend Analytics
 *
 * While other teams are working on the implementation details of these three services, you are asked to create a Kafka Streams
 * application that does following.
 * 1. Select Invoices where DeliveryType = "HOME-DELIVERY" and push them to the shipment service queue.
 * 2. Select Invoices where CustomerType = "PRIME" and create a notification event for the Loyalty Management Service. The
 * format for the new notification event is given here.
 * 3. Select all Invoices, mask the personal information, and create records for Trend Analytics. When the records are ready,
 * persist them to Hadoop storage for batch analytics. The format for the new Hadoop record is also given.
 */

public class PosFanoutApp {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String [] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> ks0 = builder.stream(AppConfigs.posTopicName,
                // specify a serde here instead of in properties because each pipeline requires different data type
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        // shipment pipeline
        ks0.filter((k, v) ->
                v.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
                .to(AppConfigs.shipmentTopicName, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        // loyalty pipeline
        ks0.filter((k, v) ->
                v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        // analytics pipeline
        ks0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
                .flatMapValues(invoice -> RecordBuilder.getHadoopRecords(invoice))
                .to(AppConfigs.hadoopTopic, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping stream");
            streams.close();
        }));
    }
}
