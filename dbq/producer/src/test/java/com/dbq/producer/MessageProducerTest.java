package com.dbq.producer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageProducerTest {
    @Test
    void routesStableKeysToStablePartitions() {
        int first = MessageProducer.selectPartition("user-42", 12, 0);
        int second = MessageProducer.selectPartition("user-42", 12, 7);

        assertEquals(first, second);
    }

    @Test
    void routesUnkeyedRecordsUsingRoundRobinSequence() {
        assertEquals(0, MessageProducer.selectPartition(null, 3, 0));
        assertEquals(1, MessageProducer.selectPartition("", 3, 1));
        assertEquals(2, MessageProducer.selectPartition(null, 3, 2));
        assertEquals(0, MessageProducer.selectPartition(null, 3, 3));
    }

    @Test
    void rejectsTopicsWithoutPartitions() {
        assertThrows(IllegalArgumentException.class, () -> MessageProducer.selectPartition("key", 0, 0));
    }
}