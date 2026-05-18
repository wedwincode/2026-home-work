package company.vk.edu.distrib.compute.wedwincode.kafka;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.KVServiceImpl;
import company.vk.edu.distrib.compute.wedwincode.exceptions.AuditException;

import java.io.IOException;
import java.util.Map;

public class AuditableKVServiceImpl extends KVServiceImpl implements AuditableKVService {

    private KafkaAuditProducer producer;

    public AuditableKVServiceImpl(int port, Dao<DaoRecord> dao) throws IOException {
        super(port, dao);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        producer = new KafkaAuditProducer(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        if (producer != null) {
            producer.setAsync(enabled);
        }
    }

    @Override
    protected void handleEntityMethod(Map<String, String> params, HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String id;

        try {
            id = getValueFromParams("id", params);
        } catch (IllegalArgumentException e) {
            throw new AuditException("id is incorrect", e);
        }

        producer.send(new AuditEvent(method, id, System.currentTimeMillis()));

        super.handleEntityMethod(params, exchange);
    }
}
