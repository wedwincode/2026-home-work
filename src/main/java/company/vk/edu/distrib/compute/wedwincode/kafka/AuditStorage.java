package company.vk.edu.distrib.compute.wedwincode.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.wedwincode.exceptions.AuditException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class AuditStorage {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;

    public AuditStorage(Path file) {
        this.file = file;
        try {
            Files.createDirectories(file.getParent());
        } catch (IOException e) {
            throw new AuditException("audit init exception", e);
        }
    }

    public void save(AuditEvent event) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            String json = MAPPER.writeValueAsString(event);
            writer.write(json);
            writer.newLine();
        } catch (IOException e) {
            throw new AuditException("audit save exception", e);
        }
    }

    public List<AuditEvent> getAll() {
        if (!Files.exists(file)) {
            return List.of();
        }

        try (var lines = Files.lines(file)) {
            return lines
                    .filter(line -> !line.isBlank())
                    .map(this::mapOne)
                    .toList();
        } catch (IOException e) {
            throw new AuditException("audit read exception", e);
        }
    }

    private AuditEvent mapOne(String line) {
        try {
            return MAPPER.readValue(line, AuditEvent.class);
        } catch (JsonProcessingException e) {
            throw new AuditException("mapping exception", e);
        }
    }
}
