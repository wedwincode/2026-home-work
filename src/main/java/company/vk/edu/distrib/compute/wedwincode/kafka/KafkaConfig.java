package company.vk.edu.distrib.compute.wedwincode.kafka;

public record KafkaConfig(
        String bootstrapServers,
        String auditTopic
) {
    public KafkaConfig fromEnv() {
        return new KafkaConfig(
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                System.getenv().getOrDefault("KAFKA_AUDIT_TOPIC", "audit")
        );
    }
}
