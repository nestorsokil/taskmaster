package com.example.taskmaster.service;

import com.example.taskmaster.config.TaskmasterMetrics;
import com.example.taskmaster.config.TaskmasterProperties;
import com.example.taskmaster.domain.Task;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class WebhookService {

    private static final Duration[] RETRY_DELAYS = {
            Duration.ofSeconds(5),
            Duration.ofSeconds(20),
            Duration.ofSeconds(60)
    };

    private final ObjectMapper objectMapper;
    private final TaskmasterProperties properties;
    private final TaskmasterMetrics metrics;
    private final HttpClient httpClient;
    private final ExecutorService executor;

    public WebhookService(ObjectMapper objectMapper,
                          TaskmasterProperties properties,
                          TaskmasterMetrics metrics) {
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(properties.webhook().httpTimeoutSeconds()))
                .build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Fires a webhook callback asynchronously if the task has a callback URL.
     * Does nothing if callbackUrl is null.
     */
    public void deliverIfConfigured(Task task) {
        if (task.callbackUrl() == null || task.callbackUrl().isBlank()) {
            return;
        }
        executor.submit(() -> deliver(task));
    }

    private void deliver(Task task) {
        String callbackUrl = task.callbackUrl();
        String body;
        try {
            body = objectMapper.writeValueAsString(new WebhookPayload(
                    task.id(),
                    task.status(),
                    task.result(),
                    task.lastError(),
                    task.queueName(),
                    task.attempts()
            ));
        } catch (Exception e) {
            log.error("Failed to serialize webhook payload for task={}", task.id(), e);
            return;
        }

        int maxAttempts = 1 + RETRY_DELAYS.length; // initial + retries
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (attempt > 0) {
                try {
                    Thread.sleep(RETRY_DELAYS[attempt - 1]);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Webhook delivery interrupted for task={}, url={}", task.id(), callbackUrl);
                    return;
                }
            }

            try {
                var requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(callbackUrl))
                        .timeout(Duration.ofSeconds(properties.webhook().httpTimeoutSeconds()))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body));

                String hmacSecret = properties.webhook().hmacSecret();
                if (hmacSecret != null && !hmacSecret.isBlank()) {
                    String signature = computeHmac(hmacSecret, body);
                    requestBuilder.header("X-Webhook-Signature", "sha256=" + signature);
                }

                HttpResponse<String> response = httpClient.send(
                        requestBuilder.build(),
                        HttpResponse.BodyHandlers.ofString()
                );

                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    metrics.webhookDelivered(task.queueName(), task.status());
                    log.info("Webhook delivered for task={}, url={}, status={}", task.id(), callbackUrl, response.statusCode());
                    return;
                }

                log.info("Webhook delivery got non-2xx for task={}, url={}, httpStatus={}, attempt={}/{}",
                        task.id(), callbackUrl, response.statusCode(), attempt + 1, maxAttempts);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Webhook delivery interrupted for task={}, url={}", task.id(), callbackUrl);
                return;
            } catch (Exception e) {
                log.info("Webhook delivery failed for task={}, url={}, attempt={}/{}, error={}",
                        task.id(), callbackUrl, attempt + 1, maxAttempts, e.getMessage());
            }
        }

        log.warn("Webhook delivery exhausted all retries: task={}, url={}", task.id(), callbackUrl);
        metrics.webhookDeliveryFailed(task.queueName());
    }

    private static String computeHmac(String secret, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(body.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compute HMAC-SHA256", e);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record WebhookPayload(UUID taskId, String status, String result, String lastError, String queueName, int attempts) {}
}
