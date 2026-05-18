package company.vk.edu.distrib.compute.wedwincode.kafka;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class EdwinAuditServiceFactory extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new AuditServiceImpl(bootstrapServers, consumerGroupId, getStorage(consumerGroupId));
    }

    private AuditStorage getStorage(String consumerGroupId) {
        if (consumerGroupId == null || consumerGroupId.isEmpty()) {
            throw new IllegalArgumentException("consumerGroupId should be valid");
        }

        try {
            Path tempDir = Files.createTempDirectory("audit-test-" + consumerGroupId);
            Path path = tempDir.resolve("audit.log");
            return new AuditStorage(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
