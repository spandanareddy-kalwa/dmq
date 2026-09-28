package com.dbq.broker.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StorageManager implements AutoCloseable {
    private static final Pattern PARTITION_DIRECTORY = Pattern.compile("partition-(\\d+)");

    private final Path dataDirectory;
    private final long segmentBytes;
    private final int maxRecordBytes;
    private final Map<PartitionKey, PartitionLog> logs = new HashMap<>();

    public StorageManager(Path dataDirectory, long segmentBytes, int maxRecordBytes) throws IOException {
        this.dataDirectory = dataDirectory;
        this.segmentBytes = segmentBytes;
        this.maxRecordBytes = maxRecordBytes;
        Files.createDirectories(dataDirectory);
        loadExistingPartitions();
    }

    public synchronized PartitionLog partitionLog(String topic, int partition) throws IOException {
        PartitionKey key = new PartitionKey(topic, partition);
        PartitionLog existing = logs.get(key);
        if (existing != null) {
            return existing;
        }
        PartitionLog opened = new PartitionLog(dataDirectory, topic, partition, segmentBytes, maxRecordBytes);
        logs.put(key, opened);
        return opened;
    }

        public synchronized List<LocalPartition> localPartitions() {
        return logs.entrySet().stream()
            .map(entry -> new LocalPartition(entry.getKey().topic(), entry.getKey().partition(),
                entry.getValue().logStartOffset(), entry.getValue().endOffset()))
            .sorted(java.util.Comparator.comparing(LocalPartition::topic)
                .thenComparingInt(LocalPartition::partition))
            .toList();
        }

    private void loadExistingPartitions() throws IOException {
        try (DirectoryStream<Path> topics = Files.newDirectoryStream(dataDirectory)) {
            for (Path topicDirectory : topics) {
                if (!Files.isDirectory(topicDirectory)) {
                    continue;
                }
                String topic = topicDirectory.getFileName().toString();
                try (DirectoryStream<Path> partitions = Files.newDirectoryStream(topicDirectory)) {
                    for (Path partitionDirectory : partitions) {
                        Matcher matcher = PARTITION_DIRECTORY.matcher(partitionDirectory.getFileName().toString());
                        if (!Files.isDirectory(partitionDirectory) || !matcher.matches()) {
                            continue;
                        }
                        int partition = Integer.parseInt(matcher.group(1));
                        logs.put(new PartitionKey(topic, partition), new PartitionLog(
                                dataDirectory, topic, partition, segmentBytes, maxRecordBytes));
                    }
                }
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        IOException failure = null;
        for (PartitionLog log : logs.values()) {
            try {
                log.close();
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        logs.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private record PartitionKey(String topic, int partition) {
    }

    public record LocalPartition(String topic, int partition, long logStartOffset, long logEndOffset) {
    }
}