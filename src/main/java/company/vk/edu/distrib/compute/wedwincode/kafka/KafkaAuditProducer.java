package company.vk.edu.distrib.compute.wedwincode.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.wedwincode.exceptions.AuditException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAuditProducer {

    private static final String TOPIC = "audit";

    private final AtomicBoolean async = new AtomicBoolean();
    private final KafkaProducer<String, AuditEvent> producer;

    public KafkaAuditProducer(String bootstrapServers) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AuditEventSerializer.class.getName());

        producer = new KafkaProducer<>(producerProperties);
    }

    public void setAsync(boolean async) {
        this.async.set(async);
    }

    public void send(AuditEvent event) {

        ProducerRecord<String, AuditEvent> record = new ProducerRecord<>(TOPIC, event.id(), event);

        if (async.get()) {
            producer.send(record);
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new AuditException("Failed to send audit event", e);
            }
        }
    }

    public void stop() {
        producer.close();
    }
}
