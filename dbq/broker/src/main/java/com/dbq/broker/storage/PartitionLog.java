package com.dbq.broker.storage;

import com.dbq.proto.MessageRecord;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/** A single-writer append-only log for one topic partition. */
public final class PartitionLog implements AutoCloseable {
    private static final String SEGMENT_PREFIX = "segment-";
    private static final String SEGMENT_SUFFIX = ".log";

    private final String topic;
    private final int partition;
    private final Path directory;
    private final long maxSegmentBytes;
    private final int maxRecordBytes;
    private final List<Segment> segments = new ArrayList<>();
    private final TreeMap<Long, RecordLocation> offsetIndex = new TreeMap<>();
    private final Map<String, Long> messageOffsets = new java.util.HashMap<>();
    private long nextOffset;
    private boolean closed;

    /** Opens a partition log, recovering valid records and truncating a damaged tail. */
    public PartitionLog(Path dataDirectory, String topic, int partition,
                       long maxSegmentBytes, int maxRecordBytes) throws IOException {
        if (topic == null || !topic.matches("[A-Za-z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("Topic contains invalid path characters");
        }
        if (partition < 0 || maxSegmentBytes <= 0 || maxRecordBytes <= 0) {
            throw new IllegalArgumentException("Partition and size limits must be positive");
        }
        this.topic = topic;
        this.partition = partition;
        this.directory = dataDirectory.resolve(topic).resolve("partition-" + partition);
        this.maxSegmentBytes = maxSegmentBytes;
        this.maxRecordBytes = maxRecordBytes;
        Files.createDirectories(directory);
        recover();
    }

    /** Appends a batch, assigning consecutive offsets and forcing each record to disk. */
    public synchronized List<MessageRecord> append(List<MessageRecord> messages) throws IOException {
        ensureOpen();
        Objects.requireNonNull(messages, "messages");
        List<MessageRecord> appended = new ArrayList<>(messages.size());
        for (MessageRecord message : messages) {
            if (!message.getMessageId().isBlank()) {
                Long existingOffset = messageOffsets.get(message.getMessageId());
                if (existingOffset != null) {
                    MessageRecord existing;
                    try {
                        existing = fetch(existingOffset, 1, 0).get(0);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while checking duplicate message id", exception);
                    }
                    if (!sameMessage(existing, message)) {
                        throw new IOException("Message id was reused with different contents");
                    }
                    appended.add(existing);
                    continue;
                }
            }
            MessageRecord record = prepareRecord(Objects.requireNonNull(message, "message"), nextOffset);
            byte[] encoded = record.toByteArray();
            if (encoded.length > maxRecordBytes) {
                throw new IOException("Encoded message exceeds configured maximum record size");
            }

            int frameBytes = Integer.BYTES + encoded.length + Integer.BYTES;
            Segment active = segments.get(segments.size() - 1);
            if (active.channel.size() > 0 && active.channel.size() + frameBytes > maxSegmentBytes) {
                active = createSegment(nextOffset);
            }

            long position = active.channel.size();
            long writePosition = position;
            ByteBuffer frame = ByteBuffer.allocate(frameBytes);
            frame.putInt(encoded.length).put(encoded).putInt(crc(encoded)).flip();
            while (frame.hasRemaining()) {
                int written = active.channel.write(frame, writePosition);
                if (written <= 0) {
                    throw new IOException("Unable to make progress writing partition log");
                }
                writePosition += written;
            }
            active.channel.force(true);
            offsetIndex.put(nextOffset, new RecordLocation(active, position, encoded.length));
            messageOffsets.put(record.getMessageId(), nextOffset);
            active.maxTimestamp = Math.max(active.maxTimestamp, record.getTimestamp());
            appended.add(record);
            nextOffset++;
        }
        notifyAll();
        return List.copyOf(appended);
    }

    /** Reads up to maxMessages beginning at offset, optionally waiting for new records. */
    public synchronized List<MessageRecord> fetch(long offset, int maxMessages,
                                                   long waitMs) throws IOException, InterruptedException {
        ensureOpen();
        if (offset < 0 || maxMessages < 0 || waitMs < 0) {
            throw new IllegalArgumentException("Offset, message count, and wait must be non-negative");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMs);
        while (offset >= nextOffset && waitMs > 0) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            ensureOpen();
        }

        List<MessageRecord> records = new ArrayList<>(Math.min(maxMessages, 256));
        for (Map.Entry<Long, RecordLocation> entry : offsetIndex.tailMap(offset).entrySet()) {
            if (records.size() == maxMessages) {
                break;
            }
            RecordLocation location = entry.getValue();
            records.add(readRecord(location));
        }
        return List.copyOf(records);
    }

    public synchronized long endOffset() {
        return nextOffset;
    }

    public synchronized int segmentCount() {
        return segments.size();
    }

    public synchronized long logStartOffset() {
        return offsetIndex.isEmpty() ? nextOffset : offsetIndex.firstKey();
    }

    public synchronized void truncateTo(long exclusiveOffset) throws IOException {
        ensureOpen();
        long startOffset = logStartOffset();
        if (exclusiveOffset < startOffset || exclusiveOffset > nextOffset) {
            throw new IllegalArgumentException("Truncation offset is outside the retained log range");
        }
        if (exclusiveOffset == nextOffset) {
            return;
        }
        RecordLocation firstRemoved = offsetIndex.get(exclusiveOffset);
        if (firstRemoved == null) {
            throw new IllegalArgumentException("Truncation offset is not present in the log");
        }

        int keepThrough = segments.indexOf(firstRemoved.segment);
        for (int index = segments.size() - 1; index > keepThrough; index--) {
            Segment removed = segments.remove(index);
            removed.channel.close();
            Files.deleteIfExists(removed.path);
        }
        firstRemoved.segment.channel.truncate(firstRemoved.framePosition);
        offsetIndex.tailMap(exclusiveOffset, true).clear();
        messageOffsets.entrySet().removeIf(entry -> entry.getValue() >= exclusiveOffset);
        nextOffset = exclusiveOffset;
        firstRemoved.segment.maxTimestamp = Long.MIN_VALUE;
        for (Map.Entry<Long, RecordLocation> entry : offsetIndex.entrySet()) {
            if (entry.getValue().segment == firstRemoved.segment) {
                firstRemoved.segment.maxTimestamp = Math.max(firstRemoved.segment.maxTimestamp,
                        readRecord(entry.getValue()).getTimestamp());
            }
        }
        firstRemoved.segment.channel.force(true);
    }

    public synchronized int deleteExpiredSegments(long retentionMs, long nowMs) throws IOException {
        ensureOpen();
        if (retentionMs <= 0 || segments.isEmpty()) {
            return 0;
        }
        long cutoff = nowMs - retentionMs;
        Segment active = segments.get(segments.size() - 1);
        if (active.channel.size() > 0 && active.maxTimestamp < cutoff) {
            createSegment(nextOffset);
        }

        int deleted = 0;
        while (segments.size() > 1) {
            Segment oldest = segments.get(0);
            if (oldest.maxTimestamp >= cutoff) {
                break;
            }
            oldest.channel.close();
            Files.deleteIfExists(oldest.path);
            segments.remove(0);
            offsetIndex.entrySet().removeIf(entry -> entry.getValue().segment == oldest);
            messageOffsets.entrySet().removeIf(entry -> !offsetIndex.containsKey(entry.getValue()));
            deleted++;
        }
        return deleted;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        for (Segment segment : segments) {
            try {
                segment.channel.close();
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        notifyAll();
        if (failure != null) {
            throw failure;
        }
    }

    private MessageRecord prepareRecord(MessageRecord source, long offset) {
        ByteString payload = source.getPayload();
        CRC32C checksum = new CRC32C();
        checksum.update(payload.asReadOnlyByteBuffer());
        return source.toBuilder()
                .setMessageId(source.getMessageId().isBlank() ? UUID.randomUUID().toString() : source.getMessageId())
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .setTimestamp(source.getTimestamp() == 0 ? System.currentTimeMillis() : source.getTimestamp())
                .setSize(payload.size())
                .setChecksum(Long.toHexString(checksum.getValue()))
                .build();
    }

    private void recover() throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory,
                SEGMENT_PREFIX + "*" + SEGMENT_SUFFIX)) {
            for (Path file : stream) {
                files.add(file);
            }
        }
        files.sort(Comparator.comparing(Path::getFileName));

        boolean discardLaterSegments = false;
        for (int index = 0; index < files.size(); index++) {
            Path file = files.get(index);
            if (discardLaterSegments) {
                Files.deleteIfExists(file);
                continue;
            }
            Segment segment = new Segment(file);
            segments.add(segment);
            long fileSize = segment.channel.size();
            long position = 0;
            long validBytes = 0;
            while (position + Integer.BYTES <= fileSize) {
                ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
                if (!readFully(segment.channel, lengthBuffer, position)) {
                    break;
                }
                int recordLength = lengthBuffer.getInt(0);
                long frameEnd = position + Integer.BYTES + (long) recordLength + Integer.BYTES;
                if (recordLength <= 0 || recordLength > maxRecordBytes || frameEnd > fileSize) {
                    break;
                }
                ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
                ByteBuffer checksumBuffer = ByteBuffer.allocate(Integer.BYTES);
                if (!readFully(segment.channel, recordBuffer, position + Integer.BYTES)
                        || !readFully(segment.channel, checksumBuffer, position + Integer.BYTES + recordLength)) {
                    break;
                }
                byte[] encoded = recordBuffer.array();
                if (checksumBuffer.getInt(0) != crc(encoded)) {
                    break;
                }
                MessageRecord record;
                try {
                    record = MessageRecord.parseFrom(encoded);
                } catch (com.google.protobuf.InvalidProtocolBufferException exception) {
                    break;
                }
                if (!record.getTopic().equals(topic) || record.getPartition() != partition
                        || record.getOffset() != nextOffset) {
                    break;
                }
                offsetIndex.put(nextOffset, new RecordLocation(segment, position, recordLength));
                if (!record.getMessageId().isBlank()) {
                    messageOffsets.putIfAbsent(record.getMessageId(), nextOffset);
                }
                segment.maxTimestamp = Math.max(segment.maxTimestamp, record.getTimestamp());
                nextOffset++;
                position = frameEnd;
                validBytes = position;
            }
            if (validBytes != fileSize) {
                segment.channel.truncate(validBytes);
                segment.channel.force(true);
                discardLaterSegments = true;
            }
        }
        if (segments.isEmpty()) {
            createSegment(nextOffset);
        }
    }

    private Segment createSegment(long baseOffset) throws IOException {
        Path path = directory.resolve(SEGMENT_PREFIX + String.format("%020d", baseOffset) + SEGMENT_SUFFIX);
        Segment segment = new Segment(path);
        segments.add(segment);
        return segment;
    }

    private static int crc(byte[] bytes) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return (int) checksum.getValue();
    }

    private MessageRecord readRecord(RecordLocation location) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(location.recordLength);
        if (!readFully(location.segment.channel, data, location.framePosition + Integer.BYTES)) {
            throw new IOException("Unexpected end of segment while reading partition log");
        }
        return MessageRecord.parseFrom(data.array());
    }

    private static boolean sameMessage(MessageRecord existing, MessageRecord retry) {
        return existing.getKey().equals(retry.getKey())
                && existing.getPayload().equals(retry.getPayload())
                && existing.getHeadersMap().equals(retry.getHeadersMap());
    }

    private static boolean readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read <= 0) {
                return false;
            }
            position += read;
        }
        buffer.flip();
        return true;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Partition log is closed");
        }
    }

    private static final class Segment {
        private final Path path;
        private final FileChannel channel;
        private long maxTimestamp = Long.MIN_VALUE;

        private Segment(Path path) throws IOException {
            this.path = path;
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
    }

    private record RecordLocation(Segment segment, long framePosition, int recordLength) {
    }
}