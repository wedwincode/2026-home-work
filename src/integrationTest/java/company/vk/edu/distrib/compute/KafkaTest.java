package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.http.HttpClient;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
public class KafkaTest extends TestBase {
    static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.8")
    );

    @Test
    void succeed() {
        String bootstrapServers = KAFKA.getBootstrapServers();
        assertNotNull(bootstrapServers);
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }
}
