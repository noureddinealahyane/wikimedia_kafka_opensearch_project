import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class ApiToKafkaProducer {

    // Kafka topic name
    private static final String TOPIC = "api-data-topic";

    public static void main(String[] args) {
        // Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", 10); // Batch messages for better performance
        props.put("batch.size", 16384); // Increase batch size for efficiency

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producer...");
            producer.close();
        }));

        // API URL
        String apiUrl = "https://stream.wikimedia.org/v2/stream/recentchange"; // SSE stream API

        while (true) {
            try {
                // Connect to the API and read the event stream
                URL url = new URL(apiUrl);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setRequestProperty("Accept", "text/event-stream");

                // Read the stream continuously
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.isEmpty() && !line.startsWith(":")) { // Ignore comments or empty lines
                            // Send each event to Kafka
                            producer.send(new ProducerRecord<>(TOPIC, null, line), (metadata, exception) -> {
                                if (exception != null) {
                                    System.err.println("Failed to send message to Kafka: " + exception.getMessage());
                                } else {
                                    System.out.println("Message sent to Kafka topic " + metadata.topic() + ", partition: " + metadata.partition());
                                }
                            });
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error connecting to API or sending data to Kafka: " + e.getMessage());
                try {
                    System.out.println("Retrying in 5 seconds...");
                    Thread.sleep(5000); // Retry after a delay
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.err.println("Interrupted during retry delay: " + ie.getMessage());
                    break;
                }
            }
        }
    }
}