package company.vk.edu.distrib.compute.wedwincode.exceptions;

import java.io.Serial;

public class AuditException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 8288999656559980882L;

    public AuditException(String message, Throwable cause) {
        super(message, cause);
    }
}
