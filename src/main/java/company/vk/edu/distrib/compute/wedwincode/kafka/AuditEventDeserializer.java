package company.vk.edu.distrib.compute.wedwincode.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class AuditEventDeserializer implements Deserializer<AuditEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public AuditEvent deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), AuditEvent.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to AuditEvent");
        }
    }
}
