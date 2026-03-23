package io.github.nestorsokil.taskmaster.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Webhook callback delivery for tasks reaching terminal states.
 */
@Tag("integration")
class WebhookIT {

    private static final String HMAC_SECRET = "test-webhook-secret";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TaskmasterClient client = new TaskmasterClient();

    private HttpServer callbackServer;
    private String callbackUrl;
    private final List<ReceivedCallback> receivedCallbacks = new CopyOnWriteArrayList<>();

    record ReceivedCallback(String body, Map<String, String> headers) {}

    @BeforeEach
    void startCallbackServer() throws IOException {
        callbackServer = HttpServer.create(new InetSocketAddress(0), 0);
        int port = callbackServer.getAddress().getPort();
        callbackUrl = "http://localhost:" + port + "/webhook";

        callbackServer.createContext("/webhook", exchange -> {
            var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            var headers = new HashMap<String, String>();
            exchange.getRequestHeaders().forEach((key, values) ->
                    headers.put(key.toLowerCase(), values.getFirst()));
            receivedCallbacks.add(new ReceivedCallback(body, headers));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        callbackServer.start();
    }

    @AfterEach
    void stopCallbackServer() {
        if (callbackServer != null) {
            callbackServer.stop(0);
        }
    }

    private String uniqueQueue() {
        return "webhook-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void callbackFiredOnComplete() throws Exception {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var submitted = client.submitTask(queue, Map.of("data", "test"), 0, null, null, null, callbackUrl);
        client.claimTasks(workerId, queue, 1);
        client.completeTask(submitted.taskId(), workerId, "all-good");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(receivedCallbacks).hasSize(1));

        var payload = MAPPER.readTree(receivedCallbacks.getFirst().body());
        assertThat(payload.get("taskId").asText()).isEqualTo(submitted.taskId().toString());
        assertThat(payload.get("status").asText()).isEqualTo("DONE");
        assertThat(payload.get("result").asText()).isEqualTo("all-good");
        assertThat(payload.get("queueName").asText()).isEqualTo(queue);
        assertThat(payload.get("attempts").asInt()).isEqualTo(1);
        assertThat(payload.has("lastError")).isFalse();
    }

    @Test
    void callbackFiredOnDead() throws Exception {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var submitted = client.submitTask(queue, Map.of("data", "test"), 0, 1, null, null, callbackUrl);
        client.claimTasks(workerId, queue, 1);
        client.failTask(submitted.taskId(), workerId, "boom");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(receivedCallbacks).hasSize(1));

        var payload = MAPPER.readTree(receivedCallbacks.getFirst().body());
        assertThat(payload.get("taskId").asText()).isEqualTo(submitted.taskId().toString());
        assertThat(payload.get("status").asText()).isEqualTo("DEAD");
        assertThat(payload.get("lastError").asText()).isEqualTo("boom");
        assertThat(payload.get("queueName").asText()).isEqualTo(queue);
        assertThat(payload.has("result")).isFalse();
    }

    @Test
    void callbackIncludesValidHmacSignature() throws Exception {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        var submitted = client.submitTask(queue, Map.of("data", "hmac-test"), 0, null, null, null, callbackUrl);
        client.claimTasks(workerId, queue, 1);
        client.completeTask(submitted.taskId(), workerId, "signed");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(receivedCallbacks).hasSize(1));

        var received = receivedCallbacks.getFirst();
        String signatureHeader = received.headers().get("x-webhook-signature");
        assertThat(signatureHeader).startsWith("sha256=");

        String expectedSignature = computeHmac(HMAC_SECRET, received.body());
        assertThat(signatureHeader).isEqualTo("sha256=" + expectedSignature);
    }

    @Test
    void callbackUrlReturnedInGetResponse() {
        var queue = uniqueQueue();
        var submitted = client.submitTask(queue, Map.of("data", "test"), 0, null, null, null, callbackUrl);

        var task = client.getTask(submitted.taskId());
        assertThat(task.callbackUrl()).isEqualTo(callbackUrl);
    }

    @Test
    void noCallbackWhenUrlAbsent() throws Exception {
        var queue = uniqueQueue();
        var workerId = "worker-" + UUID.randomUUID();
        client.registerWorker(workerId, queue);

        // Submit without callbackUrl
        var submitted = client.submitTask(queue, Map.of("data", "test"));
        client.claimTasks(workerId, queue, 1);
        client.completeTask(submitted.taskId(), workerId, "done");

        // Short wait — no callback should arrive
        Thread.sleep(1000);
        assertThat(receivedCallbacks).isEmpty();

        var task = client.getTask(submitted.taskId());
        assertThat(task.callbackUrl()).isNull();
    }

    private static String computeHmac(String secret, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(body.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
