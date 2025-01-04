import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.*;
import org.opensearch.client.indices.*;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;



public class KafkaToOpenSearch {
    private static final Logger log = LoggerFactory.getLogger(KafkaToOpenSearch.class);
    private static final String INDEX_NAME = "wikimedia_data";
    private static final String TOPIC_NAME = "api-data-topic";
    private static final String OPENSEARCH_PASSWORD = System.getenv("OPENSEARCH_INITIAL_ADMIN_PASSWORD");

    private static RestHighLevelClient createOpenSearchClient() throws Exception {
        // Create SSL context that trusts all certificates (for development only)
        SSLContext sslContext = new SSLContextBuilder()
                .loadTrustMaterial(null, (TrustStrategy) (X509Certificate[] chain, String authType) -> true)
                .build();

        // Configure basic authentication
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("admin", "Noureddine@2002"));

        // Create the client configuration
        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setSSLContext(sslContext)
                        .setSSLHostnameVerifier((hostname, session) -> true)
                        .setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(props);
    }

    private static void createIndexIfNotExists(RestHighLevelClient client) {
        try {
            boolean indexExists = client.indices().exists(
                    new GetIndexRequest(INDEX_NAME),
                    RequestOptions.DEFAULT
            );

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Created index: {}", INDEX_NAME);
            } else {
                log.info("Index already exists: {}", INDEX_NAME);
            }
        } catch (IOException e) {
            log.error("Error checking/creating index", e);
            throw new RuntimeException("Could not create index", e);
        }
    }


    public static void main(String[] args) {
        log.info("Starting the application");

        try (RestHighLevelClient openSearchClient = createOpenSearchClient();
             KafkaConsumer<String, String> consumer = createKafkaConsumer()) {

            createIndexIfNotExists(openSearchClient);
            consumer.subscribe(Collections.singleton(TOPIC_NAME));
            log.info("Subscribed to topic: {}", TOPIC_NAME);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received {} records", recordCount);

                if (recordCount > 0) {
                    BulkRequest bulkRequest = new BulkRequest();

                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            // Extract the JSON data from the SSE message
                            String rawValue = record.value();
                            String jsonData = extractJsonData(rawValue);

                            if (jsonData != null) {
                                String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                                IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                                        .source(jsonData, XContentType.JSON)
                                        .id(id);
                                bulkRequest.add(indexRequest);
                            }
                        } catch (Exception e) {
                            log.error("Error while parsing record: {}", record.value(), e);
                        }
                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        log.info("Bulk request executed. Has failures: {}", bulkResponse.hasFailures());

                        if (!bulkResponse.hasFailures()) {
                            consumer.commitSync();
                            log.info("Offsets committed");
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Application failed", e);
        }
    }

    private static String extractJsonData(String sseMessage) {
        try {
            String[] lines = sseMessage.split("\n");
            for (String line : lines) {
                if (line.startsWith("data: ")) {
                    return line.substring("data: ".length()).trim();
                }
            }
            log.warn("No data field found in SSE message");
            return null;
        } catch (Exception e) {
            log.error("Error extracting JSON data from SSE message", e);
            return null;
        }
    }
}