package company.vk.edu.distrib.compute.wedwincode.kafka;

import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.KVServiceImpl;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class AuditableKVServiceImpl extends KVServiceImpl implements AuditableKVService {

    public AuditableKVServiceImpl(int port, Dao<DaoRecord> dao) throws IOException {
        super(port, dao);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {

    }

    @Override
    public void setAsync(boolean enabled) {

    }

    @Override
    public CompletableFuture<Void> awaitTermination() {
        return super.awaitTermination();
    }
}
