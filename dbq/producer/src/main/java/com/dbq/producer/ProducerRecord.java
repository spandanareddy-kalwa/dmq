package com.dbq.producer;

import com.google.protobuf.ByteString;

import java.util.Map;

public record ProducerRecord(String key, ByteString payload, Map<String, String> headers) {
    public ProducerRecord {
        if (payload == null) {
            throw new IllegalArgumentException("Payload is required");
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static ProducerRecord of(String key, byte[] payload) {
        return new ProducerRecord(key, ByteString.copyFrom(payload), Map.of());
    }
}