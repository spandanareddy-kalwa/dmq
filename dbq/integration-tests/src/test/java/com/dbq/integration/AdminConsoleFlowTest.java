package com.dbq.integration;

import com.dbq.broker.BrokerConfig;
import com.dbq.broker.BrokerServer;
import com.dbq.coordinator.CoordinatorServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminConsoleFlowTest {
    @TempDir
    Path dataDirectory;

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void adminConsoleCreatesTopicPublishesAndFetchesCommittedMessage() throws Exception {
        CoordinatorServer coordinator = new CoordinatorServer(0, 10_000, null, 0);
        BrokerServer broker = null;
        coordinator.start();
        try {
            broker = new BrokerServer(new BrokerConfig(
                    "console-broker", "localhost", freePort(), dataDirectory,
                    "localhost", coordinator.port(), 1024 * 1024, 1024 * 1024,
                    500, 2_000));
            broker.start();
            String baseUrl = "http://localhost:" + coordinator.adminPort();
            HttpResponse<String> page = get(baseUrl + "/");
            assertEquals(200, page.statusCode());
            assertTrue(page.body().contains("Message workbench"));
            HttpResponse<String> created = post(baseUrl + "/api/topics", """
                    {"topicName":"dashboard-events","partitionCount":1,"replicationFactor":1,"retentionMs":0,"segmentSizeBytes":1048576}
                    """);
            assertEquals(201, created.statusCode());
            HttpResponse<String> published = post(baseUrl + "/api/messages", """
                    {"topic":"dashboard-events","partition":0,"key":"event-1","payload":"visible in console","ackMode":"ACK_ALL"}
                    """);
            assertEquals(200, published.statusCode());
            assertEquals(0, objectMapper.readTree(published.body()).path("baseOffset").asInt());
            HttpResponse<String> fetched = get(baseUrl + "/api/messages?topic=dashboard-events&partition=0&offset=0&limit=20");
            JsonNode pageData = objectMapper.readTree(fetched.body());
            assertEquals(200, fetched.statusCode());
            assertEquals(1, pageData.path("messages").size());
            assertEquals("visible in console", pageData.path("messages").get(0).path("payload").asText());
            JsonNode overview = objectMapper.readTree(get(baseUrl + "/api/overview").body());
            assertEquals(1, overview.path("partitions").get(0).path("highWatermark").asInt());
            assertEquals(1, overview.path("partitions").get(0).path("logEndOffset").asInt());

                HttpResponse<String> joined = post(baseUrl + "/api/groups/join", """
                    {"groupId":"dashboard-workers","consumerId":"console-consumer","topic":"dashboard-events"}
                    """);
                assertEquals(200, joined.statusCode());
                assertEquals(1, objectMapper.readTree(joined.body()).path("assignedPartitions").size());

                HttpResponse<String> polled = post(baseUrl + "/api/groups/poll", """
                    {"groupId":"dashboard-workers","consumerId":"console-consumer"}
                    """);
                JsonNode pollResult = objectMapper.readTree(polled.body());
                assertEquals(1, pollResult.path("messages").size());
                assertEquals("visible in console", pollResult.path("messages").get(0).path("payload").asText());

                JsonNode nextOffset = pollResult.path("nextOffsets").get(0);
                HttpResponse<String> committed = post(baseUrl + "/api/groups/commit", """
                    {"groupId":"dashboard-workers","consumerId":"console-consumer","topic":"dashboard-events","partition":0,"offset":1}
                    """);
                assertEquals(200, committed.statusCode());
                JsonNode afterCommit = objectMapper.readTree(get(baseUrl + "/api/overview").body());
                assertEquals(1, afterCommit.path("consumerGroups").get(0).path("memberCount").asInt());
                assertEquals(nextOffset.path("offset").asInt(),
                    afterCommit.path("committedOffsets").get(0).path("offset").asInt());
        } finally {
            if (broker != null) {
                broker.close();
            }
            coordinator.stop();
        }
    }

    private HttpResponse<String> get(String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String url, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
