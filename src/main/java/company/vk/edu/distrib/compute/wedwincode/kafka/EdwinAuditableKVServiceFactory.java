package company.vk.edu.distrib.compute.wedwincode.kafka;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.wedwincode.InMemoryDao;

import java.io.IOException;

public class EdwinAuditableKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        AuditableKVServiceImpl auditableKVService = new AuditableKVServiceImpl(port, new InMemoryDao<>());
        auditableKVService.setBootstrapServers("localhost:9092");
        auditableKVService.setAsync(false);
        return auditableKVService;
    }
}
