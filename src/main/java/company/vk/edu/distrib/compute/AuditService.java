package company.vk.edu.distrib.compute;

import java.util.List;

public interface AuditService {
    void start(String consumerGroupId);

    void stop();

    List<AuditEvent> listAuditEntries();
}
