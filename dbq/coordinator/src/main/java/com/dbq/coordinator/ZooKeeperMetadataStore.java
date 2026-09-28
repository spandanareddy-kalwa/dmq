package com.dbq.coordinator;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.PartitionMetadata;
import com.dbq.proto.TopicMetadata;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Stores coordinator metadata in ZooKeeper; message bytes remain on broker disks. */
public final class ZooKeeperMetadataStore implements AutoCloseable {
    private static final String ROOT = "/dbq";
    private static final String BROKERS = ROOT + "/brokers";
    private static final String TOPICS = ROOT + "/topics";
    private static final String OFFSETS = ROOT + "/offsets";

    private final ZooKeeper zooKeeper;

    public ZooKeeperMetadataStore(String connectString, int sessionTimeoutMs) throws IOException {
        CountDownLatch connected = new CountDownLatch(1);
        try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            if (!connected.await(sessionTimeoutMs, TimeUnit.MILLISECONDS)) {
                zooKeeper.close();
                throw new IOException("Timed out connecting to ZooKeeper at " + connectString);
            }
            ensurePersistentPath(BROKERS);
            ensurePersistentPath(TOPICS);
            ensurePersistentPath(OFFSETS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while connecting to ZooKeeper", exception);
        } catch (KeeperException exception) {
            throw new IOException("Unable to initialize ZooKeeper metadata paths", exception);
        }
    }

    public void registerBroker(BrokerInfo broker) throws IOException {
        String path = BROKERS + "/" + encode(broker.getBrokerId());
        execute("register broker", () -> {
            try {
                ensurePersistentPath(BROKERS);
                zooKeeper.create(path, broker.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException exists) {
                Stat stat = zooKeeper.exists(path, false);
                if (stat == null || stat.getEphemeralOwner() != zooKeeper.getSessionId()) {
                    throw new IOException("Broker registration is owned by another coordinator session: " + broker.getBrokerId());
                }
                zooKeeper.setData(path, broker.toByteArray(), stat.getVersion());
            }
            return null;
        });
    }

    public void updateBroker(BrokerInfo broker) throws IOException {
        registerBroker(broker);
    }

    public void removeBroker(String brokerId) throws IOException {
        delete(BROKERS + "/" + encode(brokerId));
    }

    public List<BrokerInfo> loadBrokers() throws IOException {
        return execute("load brokers", () -> {
            List<BrokerInfo> result = new ArrayList<>();
            for (String child : children(BROKERS)) {
                try {
                    BrokerInfo broker = BrokerInfo.parseFrom(zooKeeper.getData(BROKERS + "/" + child, false, null));
                    result.add(broker.toBuilder().setAlive(true).build());
                } catch (InvalidProtocolBufferException exception) {
                    throw new IOException("Invalid broker metadata at " + child, exception);
                }
            }
            return result;
        });
    }

    public void saveTopic(TopicMetadata topic) throws IOException {
        String path = topicPath(topic.getName());
        writePersistent(path, topic.toByteArray());
        execute("initialize topic partitions", () -> {
            ensurePersistentPath(path + "/partitions");
            return null;
        });
    }

    public List<TopicMetadata> loadTopics() throws IOException {
        return execute("load topics", () -> {
            List<TopicMetadata> result = new ArrayList<>();
            for (String child : children(TOPICS)) {
                try {
                    result.add(TopicMetadata.parseFrom(zooKeeper.getData(TOPICS + "/" + child, false, null)));
                } catch (InvalidProtocolBufferException exception) {
                    throw new IOException("Invalid topic metadata at " + child, exception);
                }
            }
            return result;
        });
    }

    public void savePartition(PartitionMetadata partition) throws IOException {
        String path = topicPath(partition.getTopic()) + "/partitions/" + partition.getPartitionId();
        writePersistent(path, partition.toByteArray());
    }

    public List<PartitionMetadata> loadPartitions(String topic) throws IOException {
        String partitionsPath = topicPath(topic) + "/partitions";
        return execute("load partitions", () -> {
            List<PartitionMetadata> result = new ArrayList<>();
            for (String child : children(partitionsPath)) {
                try {
                    result.add(PartitionMetadata.parseFrom(zooKeeper.getData(partitionsPath + "/" + child, false, null)));
                } catch (InvalidProtocolBufferException exception) {
                    throw new IOException("Invalid partition metadata at " + child, exception);
                }
            }
            return result;
        });
    }

    public void saveOffset(String groupId, String topic, int partition, long offset) throws IOException {
        String path = offsetPath(groupId, topic, partition);
        writePersistent(path, ByteBuffer.allocate(Long.BYTES).putLong(offset).array());
    }

    public List<CommittedOffset> loadOffsets() throws IOException {
        return execute("load offsets", () -> {
            List<CommittedOffset> result = new ArrayList<>();
            for (String group : children(OFFSETS)) {
                for (String topic : children(OFFSETS + "/" + group)) {
                    for (String partition : children(OFFSETS + "/" + group + "/" + topic)) {
                        String path = OFFSETS + "/" + group + "/" + topic + "/" + partition;
                        byte[] data = zooKeeper.getData(path, false, null);
                        if (data.length != Long.BYTES) {
                            throw new IOException("Invalid committed offset at " + path);
                        }
                        result.add(new CommittedOffset(decode(group), decode(topic),
                                Integer.parseInt(partition), ByteBuffer.wrap(data).getLong()));
                    }
                }
            }
            return result;
        });
    }

    @Override
    public void close() throws IOException {
        try {
            zooKeeper.close();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing ZooKeeper session", exception);
        }
    }

    private void writePersistent(String path, byte[] data) throws IOException {
        execute("write metadata", () -> {
            ensurePersistentPath(parent(path));
            try {
                zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException exists) {
                Stat stat = zooKeeper.exists(path, false);
                zooKeeper.setData(path, data, stat == null ? -1 : stat.getVersion());
            }
            return null;
        });
    }

    private void ensurePersistentPath(String path) throws KeeperException, InterruptedException {
        if (path == null || path.isEmpty() || path.equals("/")) {
            return;
        }
        String current = "";
        for (String part : path.substring(1).split("/")) {
            current += "/" + part;
            if (zooKeeper.exists(current, false) == null) {
                try {
                    zooKeeper.create(current, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException ignored) {
                    // Another coordinator operation created this parent concurrently.
                }
            }
        }
    }

    private void delete(String path) throws IOException {
        execute("delete metadata", () -> {
            Stat stat = zooKeeper.exists(path, false);
            if (stat != null) {
                zooKeeper.delete(path, stat.getVersion());
            }
            return null;
        });
    }

    private List<String> children(String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {
            return List.of();
        }
        return zooKeeper.getChildren(path, false);
    }

    private <T> T execute(String operation, ZkOperation<T> operationBody) throws IOException {
        try {
            return operationBody.run();
        } catch (KeeperException exception) {
            throw new IOException("ZooKeeper failed to " + operation, exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while attempting to " + operation, exception);
        }
    }

    private static String topicPath(String topic) {
        return TOPICS + "/" + encode(topic);
    }

    private static String offsetPath(String group, String topic, int partition) {
        return OFFSETS + "/" + encode(group) + "/" + encode(topic) + "/" + partition;
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static String parent(String path) {
        int separator = path.lastIndexOf('/');
        return separator <= 0 ? "/" : path.substring(0, separator);
    }

    public record CommittedOffset(String groupId, String topic, int partition, long offset) {
    }

    @FunctionalInterface
    private interface ZkOperation<T> {
        T run() throws KeeperException, InterruptedException, IOException;
    }
}
