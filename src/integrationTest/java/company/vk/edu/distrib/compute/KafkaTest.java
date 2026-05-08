package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

public class KafkaTest extends TestBase {
    static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @Test
    void succeed() {
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }
}
