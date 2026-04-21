package company.vk.edu.distrib.compute.wedwincode.replicated;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.KVServiceImpl;
import company.vk.edu.distrib.compute.wedwincode.exceptions.QuorumException;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStopException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class ReplicatedKVServiceImpl extends KVServiceImpl implements ReplicatedService {
    private static final int DEFAULT_ACK = 1;

    private final int port;
    private final List<Dao<DaoRecord>> replicas;
    private final boolean[] enabled;

    private final ParallelReplicationService parallelReplicationService;

    private record ReadResult(boolean responded, DaoRecord record) {}

    public ReplicatedKVServiceImpl(int port, List<Dao<DaoRecord>> replicas) throws IOException {
        super(port, null);
        this.port = port;
        this.replicas = replicas;
        enabled = new boolean[replicas.size()];
        Arrays.fill(enabled, true);
        parallelReplicationService = new ParallelReplicationService(replicas, i -> enabled[i]);
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

        Function<Dao<DaoRecord>, ReadResult> taskGet = (replica) -> {
            try {
                return new ReadResult(true, replica.get(id));
            } catch (NoSuchElementException e) {
                return new ReadResult(true, null);
            } catch (IOException e) {
                return new ReadResult(false, null);
            }
        };

        List<ReadResult> results = parallelReplicationService.performTaskWithResult(taskGet);

        int confirmed = 0;
        DaoRecord best = null;
        for (var result: results) {
            if (result.responded) {
                confirmed++;
            }
            if (best == null || result.record().timestamp() > best.timestamp()) {
                best = result.record();
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

        Function<Dao<DaoRecord>, Boolean> upsertion = (replica) -> {
            try {
                replica.upsert(id, DaoRecord.buildCreated(data));
                return true;
            } catch (Exception e) {
                return false;
            }
        };

        List<Boolean> results = parallelReplicationService.performTaskWithResult(upsertion);
        int confirmed = (int) results.stream().filter(Boolean.TRUE::equals).count();
        if (confirmed < ack) {
            throw new QuorumException("error upserting data");
        }
    }

    @Override
    protected void deleteValue(Map<String, String> params) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        Function<Dao<DaoRecord>, Boolean> deletion = (replica) -> {
            try {
                replica.delete(id);
                return true;
            } catch (Exception e) {
                return false;
            }
        };

        List<Boolean> results = parallelReplicationService.performTaskWithResult(deletion);
        int confirmed = (int) results.stream().filter(Boolean.TRUE::equals).count();
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
