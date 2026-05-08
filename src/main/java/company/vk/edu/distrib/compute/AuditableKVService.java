package company.vk.edu.distrib.compute;

public interface AuditableKVService extends KVService {
    void setAsync(boolean enabled);
}
