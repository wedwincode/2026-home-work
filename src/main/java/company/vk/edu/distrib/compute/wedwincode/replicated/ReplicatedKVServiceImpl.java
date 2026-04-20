package company.vk.edu.distrib.compute.wedwincode.replicated;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.KVServiceImpl;
import company.vk.edu.distrib.compute.wedwincode.exceptions.QuorumException;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ReplicatedKVServiceImpl extends KVServiceImpl implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);
    private static final int DEFAULT_ACK = 1;

    private final int port;
    private final List<Dao<DaoRecord>> replicas;
    private final boolean[] enabled;

    public ReplicatedKVServiceImpl(int port, List<Dao<DaoRecord>> replicas) throws IOException {
        super(port, null);
        this.port = port;
        this.replicas = replicas;
        enabled = new boolean[replicas.size()];
        Arrays.fill(enabled, true);
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicas.size();
    }

    @Override
    public void disableReplica(int nodeId) {
        if (nodeId > replicas.size()) {
            throw new IllegalArgumentException("incorrect nodeId");
        }
        enabled[nodeId] = false;
    }

    @Override
    public void enableReplica(int nodeId) {
        if (nodeId > replicas.size()) {
            throw new IllegalArgumentException("incorrect nodeId");
        }
        enabled[nodeId] = true;
    }

    @Override
    protected void closeDao() {
        boolean success = true;
        for (Dao<DaoRecord> replica: replicas) {
            try {
                replica.close();
            } catch (IOException e) {
                success = false;
            }
        }
        if (!success) {
            throw new ServiceStopException("one or multiple dao was not closed successfully");
        }
    }

    @Override
    protected void handleEntityMethod(Map<String, String> params, HttpExchange exchange) throws IOException {
        int ack = getAck(params);
        if (ack > replicas.size()) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return;
        }
        super.handleEntityMethod(params, exchange);
    }

    @Override
    protected DaoRecord getValue(Map<String, String> params) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        int confirmed = 0;
        DaoRecord best = null;

        for (int i = 0; i < replicas.size(); i++) {
            if (!enabled[i]) {
                continue;
            }
            try {
                DaoRecord current = replicas.get(i).get(id);
                if (best == null || current.timestamp() > best.timestamp()) {
                    best = current;
                }
                confirmed++;
                log.debug("got from replicaId={}", i);
            } catch (NoSuchElementException e) {
                confirmed++;
                log.debug("replicaId={} has no value", i);
            } catch (IOException e) {
                log.debug("replicaId={} unavailable", i, e);
            }
        }
        if (confirmed < ack) {
            throw new QuorumException("id not found");
        }
        if (best == null || best.deleted()) {
            throw new NoSuchElementException("id was deleted");
        }

        return best;
    }

    @Override
    protected void putValue(Map<String, String> params, byte[] data) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        int confirmed = 0;

        for (int i = 0; i < replicas.size(); i++) {
            if (!enabled[i]) {
                continue;
            }
            try {
                replicas.get(i).upsert(id, DaoRecord.buildCreated(data));
                confirmed++;
                log.debug("put to replicaId={}", i);
            } catch (Exception e) {
                // pass
            }
        }

        if (confirmed < ack) {
            throw new QuorumException("error upserting data");
        }
    }

    @Override
    protected void deleteValue(Map<String, String> params) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        int confirmed = 0;

        for (int i = 0; i < replicas.size(); i++) {
            if (!enabled[i]) {
                continue;
            }
            try {
                replicas.get(i).delete(id);
                confirmed++;
                log.debug("deleted from replicaId={}", i);
            } catch (Exception e) {
                // pass
            }
        }

        if (confirmed < ack) {
            throw new QuorumException("error deleting data");
        }
    }

    private static int getAck(Map<String, String> params) {
        try {
            String ackRaw = getValueFromParams("ack", params);
            return Integer.parseInt(ackRaw);
        } catch (IllegalArgumentException e) {
            return DEFAULT_ACK;
        }
    }
}
