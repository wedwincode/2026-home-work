package company.vk.edu.distrib.compute.wedwincode.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class AuditServiceImpl implements AuditService {

    private static final String TOPIC = "audit";
    private static final Duration POLL_DURATION = Duration.ofMillis(500);

    private final Properties consumerProperties = new Properties();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AuditStorage storage;
    private KafkaConsumer<String, AuditEvent> consumer;
    private CountDownLatch startedLatch;
    private Thread auditThread;

    public AuditServiceImpl(String bootstrapServers, String consumerGroupId, AuditStorage storage) {
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AuditEventDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.storage = storage;
    }

    @Override
    public void start() {
        if (running.get()) {
            return;
        }

        running.set(true);
        startedLatch = new CountDownLatch(1);

        auditThread = new Thread(this::consumeLoop, "audit-thread");
        auditThread.start();

        try {
            startedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void consumeLoop() {
        try (KafkaConsumer<String, AuditEvent> c = new KafkaConsumer<>(consumerProperties)) {
            consumer = c;
            consumer.subscribe(List.of(TOPIC));

            while (running.get()) {
                ConsumerRecords<String, AuditEvent> records = consumer.poll(POLL_DURATION);

                if (!consumer.assignment().isEmpty()) {
                    startedLatch.countDown();
                }

                for (var record : records) {
                    storage.save(record.value());
                }

                if (records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        }
    }

    @Override
    public void stop() {
        running.set(false);

        if (consumer != null) {
            consumer.wakeup();
        }

        if (auditThread != null) {
            try {
                auditThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return storage.getAll();
    }
}
