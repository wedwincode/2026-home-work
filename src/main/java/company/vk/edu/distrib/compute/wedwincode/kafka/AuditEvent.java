package company.vk.edu.distrib.compute.wedwincode.kafka;

public record AuditEvent(
        String methodName,
        String id,
        long timestamp
) {
    public AuditEvent(String requestedId, String methodName) {
        this(methodName, requestedId, System.currentTimeMillis());
    }
}
