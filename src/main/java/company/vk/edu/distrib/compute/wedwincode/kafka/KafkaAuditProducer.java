package company.vk.edu.distrib.compute.wedwincode.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAuditProducer implements AutoCloseable {
    private final KafkaProducer<String, AuditEvent> producer;
    private final String topic;

    private volatile boolean async;

    public KafkaAuditProducer(KafkaConfig config) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AuditEventSerializer.class.getName()); // todo serializer

        producer = new KafkaProducer<>(producerProperties);
        topic = config.auditTopic();
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public void send(AuditEvent event) {
        ProducerRecord<String, AuditEvent> record = new ProducerRecord<>(topic, event.id(), event);

        if (async) {
            producer.send(record);
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to send audit event", e); // todo
            }

        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
