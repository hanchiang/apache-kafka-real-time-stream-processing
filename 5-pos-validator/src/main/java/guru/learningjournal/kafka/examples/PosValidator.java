package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonDeserializer;
import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PosValidator {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        // Set up configs
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.groupID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Create consumer, producer
        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<String, PosInvoice>(consumerProps);
        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(producerProps);

        // Subscribe to topic
        consumer.subscribe(Arrays.asList(AppConfigs.sourceTopicNames));

        // Process records
        while (true) {
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, PosInvoice> record: records) {
                if (record.value().getDeliveryType().equals("HOME-DELIVERY") &&
                        record.value().getDeliveryAddress().getContactNumber().equals("")) {
                    // invalid
                    producer.send(new ProducerRecord(AppConfigs.invalidTopicName, record.value().getStoreID(), record.value()));
                    logger.info("Invalid record - " + record.value().getInvoiceNumber());
                } else {
                    // valid
                    producer.send(new ProducerRecord(AppConfigs.validTopicName, record.value().getStoreID(), record.value()));
                    logger.info("Valid record - " + record.value().getInvoiceNumber());
                }
            }
        }
    }
}
