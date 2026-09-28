package com.dbq.coordinator;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.CreateTopicRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminHttpServerTest {
    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void servesDashboardOverviewAndTopicCreationApi() throws Exception {
        CoordinatorService service = new CoordinatorService();
        service.registerBroker(BrokerInfo.newBuilder()
                .setBrokerId("broker-ui").setHost("localhost").setPort(9191).build());
        service.createTopic(CreateTopicRequest.newBuilder()
                .setTopicName("orders").setPartitionCount(2).setReplicationFactor(1).build());

        try (AdminHttpServer admin = new AdminHttpServer(0, service)) {
            admin.start();
            HttpResponse<String> page = get(admin.port(), "/");
            assertEquals(200, page.statusCode());
            assertTrue(page.body().contains("Cluster overview"));

            HttpResponse<String> overview = get(admin.port(), "/api/overview");
            assertEquals(200, overview.statusCode());
            JsonNode snapshot = objectMapper.readTree(overview.body());
            assertEquals("broker-ui", snapshot.path("brokers").get(0).path("brokerId").asText());
            assertEquals("orders", snapshot.path("topics").get(0).path("name").asText());
            assertEquals(2, snapshot.path("partitions").size());

            HttpRequest createTopic = HttpRequest.newBuilder(URI.create(url(admin.port(), "/api/topics")))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"topicName":"events","partitionCount":1,"replicationFactor":1,"retentionMs":0,"segmentSizeBytes":0}
                            """))
                    .build();
            HttpResponse<String> created = client.send(createTopic, HttpResponse.BodyHandlers.ofString());
            assertEquals(201, created.statusCode());
            assertEquals(2, service.adminOverview().topics().size());
        }
    }

    private HttpResponse<String> get(int port, String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url(port, path))).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private String url(int port, String path) {
        return "http://localhost:" + port + path;
    }
}
