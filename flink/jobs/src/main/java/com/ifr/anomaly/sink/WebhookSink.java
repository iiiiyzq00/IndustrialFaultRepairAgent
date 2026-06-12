package com.ifr.anomaly.sink;

import com.ifr.anomaly.model.IncidentEvent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Sink that POSTs aggregated IncidentEvents to the Supervisor's webhook endpoint.
 *
 * Configuration via environment / Flink config:
 *   SUPERVISOR_WEBHOOK_URL  — full URL (default http://agent-supervisor:8100/api/v1/incident)
 *   WEBHOOK_API_KEY         — X-API-Key header value
 *   WEBHOOK_TIMEOUT_MS      — connection/read timeout (default 5000)
 *   WEBHOOK_MAX_RETRIES     — number of retries on failure (default 3)
 */
public class WebhookSink extends RichSinkFunction<IncidentEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WebhookSink.class);

    private String webhookUrl;
    private String apiKey;
    private int timeoutMs;
    private int maxRetries;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.webhookUrl = getConfig("SUPERVISOR_WEBHOOK_URL",
                "http://agent-supervisor:8100/api/v1/incident");
        this.apiKey = getConfig("WEBHOOK_API_KEY", "dev-key-change-me");
        this.timeoutMs = Integer.parseInt(getConfig("WEBHOOK_TIMEOUT_MS", "5000"));
        this.maxRetries = Integer.parseInt(getConfig("WEBHOOK_MAX_RETRIES", "3"));
        LOG.info("WebhookSink initialized: url={} timeout={}ms retries={}",
                webhookUrl, timeoutMs, maxRetries);
    }

    private String getConfig(String key, String defaultValue) {
        // Check Flink global job parameters first, then system properties, then env
        try {
            org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters params =
                    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            if (params != null && params.toMap().containsKey(key)) {
                return params.toMap().get(key);
            }
        } catch (Exception ignored) { }

        String val = System.getProperty(key);
        if (val != null) return val;
        val = System.getenv(key);
        return val != null ? val : defaultValue;
    }

    @Override
    public void invoke(IncidentEvent incident, Context context) throws Exception {
        String json = incident.toJson();
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(webhookUrl).openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setRequestProperty("X-API-Key", apiKey);
                conn.setRequestProperty("Accept", "application/json");
                conn.setDoOutput(true);
                conn.setConnectTimeout(timeoutMs);
                conn.setReadTimeout(timeoutMs);

                try (OutputStream os = conn.getOutputStream()) {
                    os.write(body);
                    os.flush();
                }

                int status = conn.getResponseCode();
                if (status >= 200 && status < 300) {
                    LOG.info("Webhook delivered: incident={} status={} attempt={}",
                            incident.getIncidentId(), status, attempt);
                    return;
                }

                if (status == 429) {
                    // Rate limited — back off
                    long backoff = (long) Math.pow(2, attempt) * 500;
                    LOG.warn("Webhook rate-limited (429), backing off {}ms", backoff);
                    Thread.sleep(backoff);
                } else {
                    LOG.warn("Webhook returned non-2xx: incident={} status={} attempt={}",
                            incident.getIncidentId(), status, attempt);
                }

            } catch (Exception e) {
                lastException = e;
                LOG.warn("Webhook delivery failed (attempt {}/{}): {}",
                        attempt, maxRetries, e.getMessage());
                if (attempt < maxRetries) {
                    Thread.sleep((long) Math.pow(2, attempt) * 1000);
                }
            }
        }

        LOG.error("Webhook delivery exhausted retries for incident={}: {}",
                incident.getIncidentId(),
                lastException != null ? lastException.getMessage() : "unknown");
        // Don't rethrow — Flink won't restart; we log and drop for now.
        // Production would use a dead-letter topic.
    }
}
