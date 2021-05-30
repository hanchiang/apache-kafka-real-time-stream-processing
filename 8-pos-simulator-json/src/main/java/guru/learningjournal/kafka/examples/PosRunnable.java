package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PosRunnable implements Runnable {
    public static final Logger logger = LogManager.getLogger();
    private static final int defaultDelay = 200;
    private boolean shouldRun = true;
    private KafkaProducer producer;
    private String topicName;
    private int delay;

    PosRunnable(KafkaProducer producer, String topicName, int delay) {
        this.producer = producer;
        this.topicName = topicName;
        this.delay = delay;
        if (this.delay < defaultDelay) {
            logger.info("Delay " + this.delay + " is too low. Setting it to " + defaultDelay);
            this.delay = defaultDelay;
        }
    }

    @Override
    public void run() {
        InvoiceGenerator invoiceGenerator = InvoiceGenerator.getInstance();
        PosInvoice invoice = invoiceGenerator.getNextInvoice();

        while (this.shouldRun) {
            logger.info("Sending invoice to kafka");
            producer.send(new ProducerRecord(this.topicName, null, invoice));
            logger.info("Finished sending invoice to kafka");
            try {
                logger.info("Sleeping for " + this.delay + "ms");
                Thread.sleep(this.delay);
            } catch (InterruptedException e) {
                logger.error(e.getStackTrace());
                this.producer.close();
                throw new RuntimeException(e);
            }
        }
        this.producer.close();
    }

    public void stop() {
        logger.info("Stopping thread");
        this.shouldRun = false;
    }
}
