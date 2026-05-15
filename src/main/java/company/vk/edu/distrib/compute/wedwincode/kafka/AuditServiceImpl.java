package company.vk.edu.distrib.compute.wedwincode.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;

import java.util.List;

public class AuditServiceImpl implements AuditService {
    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.of();
    }
}
