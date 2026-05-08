package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KafkaTest extends TestBase {
    static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Test
    void succeed() {
        assertNotNull(HTTP_CLIENT);
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }
}
