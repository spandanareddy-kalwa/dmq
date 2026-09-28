package com.dbq.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumerConfigTest {
    @Test
    void rejectsInvalidLimits() {
        assertThrows(IllegalArgumentException.class, () -> new ConsumerConfig(
                "localhost", 9090, "group", "consumer", 0, 100, 1000, 500));
    }
}