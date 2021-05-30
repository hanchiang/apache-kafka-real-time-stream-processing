package guru.learningjournal.kafka.examples;

public class AppConfigs {
    final static int delayBetweenMessage = 200; // milliseconds
    final static int numThreads = 3;
    final static String bootstrapServers = "localhost:9092";
    final static String applicationId = "posJson";
    final static String topicName = "pos";
}
