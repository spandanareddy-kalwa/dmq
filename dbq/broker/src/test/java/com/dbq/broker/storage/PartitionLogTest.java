package com.dbq.broker.storage;

import com.dbq.proto.MessageRecord;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionLogTest {
    @TempDir
    Path dataDirectory;

    @Test
    void appendsRollsSegmentsAndRecoversOffsets() throws Exception {
        try (PartitionLog log = new PartitionLog(dataDirectory, "orders", 0, 100, 1024)) {
            List<MessageRecord> appended = log.append(List.of(message("one"), message("two")));

            assertEquals(0, appended.get(0).getOffset());
            assertEquals(1, appended.get(1).getOffset());
            assertEquals("orders", appended.get(0).getTopic());
            assertFalse(appended.get(0).getChecksum().isBlank());
            assertEquals(2, log.endOffset());
            assertEquals(2, log.fetch(0, 10, 0).size());
            assertTrue(log.segmentCount() >= 2);
        }

        try (PartitionLog recovered = new PartitionLog(dataDirectory, "orders", 0, 100, 1024)) {
            assertEquals(2, recovered.endOffset());
            assertEquals("one", recovered.fetch(0, 1, 0).get(0).getKey());
            assertEquals(1, recovered.fetch(1, 1, 0).get(0).getOffset());
        }
    }

    @Test
    void truncatesIncompleteTailDuringRecovery() throws Exception {
        Path segment;
        try (PartitionLog log = new PartitionLog(dataDirectory, "events", 2, 4096, 1024)) {
            log.append(List.of(message("valid")));
        }
        segment = dataDirectory.resolve("events/partition-2/segment-00000000000000000000.log");
        long validSize = Files.size(segment);
        Files.write(segment, new byte[]{0, 0, 0}, StandardOpenOption.APPEND);

        try (PartitionLog recovered = new PartitionLog(dataDirectory, "events", 2, 4096, 1024)) {
            assertEquals(1, recovered.endOffset());
            assertEquals(validSize, Files.size(segment));
        }
    }

    @Test
    void longPollWakesWhenRecordArrives() throws Exception {
        try (PartitionLog log = new PartitionLog(dataDirectory, "events", 0, 4096, 1024)) {
            Thread writer = new Thread(() -> {
                try {
                    log.append(List.of(message("wake")));
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            });
            writer.start();

            assertEquals(1, log.fetch(0, 1, 500).size());
            writer.join();
        }
    }

    @Test
    void repeatedMessageIdReturnsOriginalOffsetWithoutAppendingAgain() throws Exception {
        MessageRecord record = message("retry").toBuilder().setMessageId("producer-1-7").build();
        try (PartitionLog log = new PartitionLog(dataDirectory, "events", 0, 4096, 1024)) {
            assertEquals(0, log.append(List.of(record)).get(0).getOffset());
            assertEquals(0, log.append(List.of(record)).get(0).getOffset());
            assertEquals(1, log.endOffset());
        }
        try (PartitionLog recovered = new PartitionLog(dataDirectory, "events", 0, 4096, 1024)) {
            assertEquals(0, recovered.append(List.of(record)).get(0).getOffset());
            assertEquals(1, recovered.endOffset());
        }
    }

    @Test
    void retentionDeletesOnlyExpiredSealedSegmentsAndAdvancesStartOffset() throws Exception {
        try (PartitionLog log = new PartitionLog(dataDirectory, "events", 0, 1, 1024)) {
            log.append(List.of(message("expired").toBuilder().setTimestamp(1_000).build()));
            log.append(List.of(message("retained").toBuilder().setTimestamp(9_000).build()));

            assertEquals(1, log.deleteExpiredSegments(5_000, 10_000));
            assertEquals(1, log.logStartOffset());
            assertEquals(2, log.endOffset());
            assertEquals("retained", log.fetch(0, 10, 0).get(0).getKey());
        }
    }

    @Test
    void truncatesUncommittedTailAndRecoversTheCommittedEnd() throws Exception {
        try (PartitionLog log = new PartitionLog(dataDirectory, "events", 0, 1, 1024)) {
            log.append(List.of(message("one"), message("two"), message("uncommitted")));
            log.truncateTo(2);
            assertEquals(2, log.endOffset());
            assertEquals(2, log.fetch(0, 10, 0).size());
        }
        try (PartitionLog recovered = new PartitionLog(dataDirectory, "events", 0, 1, 1024)) {
            assertEquals(2, recovered.endOffset());
        }
    }

    private MessageRecord message(String key) {
        return MessageRecord.newBuilder()
                .setKey(key)
                .setPayload(ByteString.copyFromUtf8("payload-" + key))
                .build();
    }
}