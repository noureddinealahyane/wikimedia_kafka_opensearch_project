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

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // API URL
        String apiUrl = "https://stream.wikimedia.org/v2/stream/recentchange"; // SSE stream API

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
                        producer.send(new ProducerRecord<>(TOPIC, null, line));
                        System.out.println("Sent to Kafka: " + line);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            producer.close();
        }
    }
}
