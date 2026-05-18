package company.vk.edu.distrib.compute.wedwincode.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AuditEventSerializer implements Serializer<AuditEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, AuditEvent data) {
        try {
            if (data == null) {
                return new byte[]{};
            }

            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing AuditEvent to byte[]", e);
        }
    }
}
