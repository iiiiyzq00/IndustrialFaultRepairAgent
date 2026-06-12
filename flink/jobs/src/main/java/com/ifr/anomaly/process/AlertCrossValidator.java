package com.ifr.anomaly.process;

import com.ifr.anomaly.model.AlertEvent;
import com.ifr.anomaly.model.MetricEvent;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * KeyedCoProcessFunction that cross-validates anomaly metrics with external alerts.
 *
 * Keyed by node_id — processes two streams simultaneously:
 *   Stream 1: MetricEvent (anomaly candidates from z-score + de-jitter)
 *   Stream 2: AlertEvent  (external alerts from monitoring systems)
 *
 * Cross-validation logic:
 *   - When a metric anomaly arrives and a recent alert exists for the same node,
 *     the metric is marked as cross-validated (higher confidence).
 *   - When an alert arrives and a recent metric anomaly exists for the same node,
 *     the metric anomaly is immediately forwarded (de-jitter bypass).
 *   - State TTL of 60 seconds via processing-time timer ensures stale state
 *     doesn't cause false cross-validations.
 */
public class AlertCrossValidator
        extends KeyedCoProcessFunction<String, MetricEvent, AlertEvent, MetricEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertCrossValidator.class);

    /** Max age (ms) for cross-referencing alert ↔ anomaly. */
    private static final long CROSS_WINDOW_MS = 60_000;

    // ---- State ----

    /** Most recent anomaly candidate metric for this node. */
    private transient ValueState<MetricEvent> lastAnomalyMetric;

    /** Most recent external alert for this node. */
    private transient ValueState<AlertEvent> lastAlert;

    /** Accumulated alert IDs to attach to outgoing metrics. */
    private transient ValueState<List<String>> pendingAlertIds;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        lastAnomalyMetric = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-anomaly-metric", MetricEvent.class));
        lastAlert = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-alert", AlertEvent.class));
        pendingAlertIds = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-alert-ids",
                        org.apache.flink.api.common.typeinfo.Types.LIST(
                                org.apache.flink.api.common.typeinfo.Types.STRING)));
    }

    // ── MetricEvent stream (anomaly candidates) ────────────────

    @Override
    public void processElement1(MetricEvent metric, Context ctx, Collector<MetricEvent> out)
            throws Exception {

        if (!metric.isAnomalyCandidate()) {
            // Non-anomalous metrics: just forward
            out.collect(metric);
            return;
        }

        // Store this anomaly for future alert cross-reference
        lastAnomalyMetric.update(metric);

        // Register cleanup timer
        long cleanupTime = ctx.timerService().currentProcessingTime() + CROSS_WINDOW_MS;
        ctx.timerService().registerProcessingTimeTimer(cleanupTime);

        // Check if there's a recent alert for this node
        AlertEvent recentAlert = lastAlert.value();
        List<String> alertIds = pendingAlertIds.value();
        if (alertIds == null) alertIds = new ArrayList<>();

        if (recentAlert != null) {
            long age = metric.getTimestamp() - recentAlert.getTimestamp();
            if (Math.abs(age) <= CROSS_WINDOW_MS) {
                // Cross-validated! Boost confidence
                metric.setAnomalyCandidate(true);
                // Use the anomaly's own deviation for forwarding — the alert
                // merely confirms the anomaly is real.
                alertIds.add(recentAlert.getAlertId());
                pendingAlertIds.update(alertIds);

                LOG.info("Alert cross-validated: node={} metric={} alert={} sigma={:.2f}",
                        metric.getNodeId(), metric.getMetricType(),
                        recentAlert.getAlertId(), metric.getDeviationSigma());

                // Attach alert IDs as a JSON-encoded tags field (reuse anomalyActive for simplicity)
                metric.setAnomalyActive("cross-validated:" + String.join(",", alertIds));
            } else {
                LOG.debug("Alert too old for cross-validation: node={} alert_age={}ms",
                        metric.getNodeId(), age);
            }
        }

        // Attach pending alert IDs
        if (!alertIds.isEmpty()) {
            metric.setAnomalyActive(
                    (metric.getAnomalyActive() != null ? metric.getAnomalyActive() + "|" : "")
                    + "alerts:" + String.join(",", alertIds));
        }

        out.collect(metric);
    }

    // ── AlertEvent stream (external alerts) ────────────────────

    @Override
    public void processElement2(AlertEvent alert, Context ctx, Collector<MetricEvent> out)
            throws Exception {

        LOG.info("External alert received: id={} node={} severity={} scenario={}",
                alert.getAlertId(), alert.getNodeId(), alert.getSeverity(),
                alert.getAnomalyScenario());

        // Store this alert for future anomaly cross-reference
        lastAlert.update(alert);

        // Accumulate alert ID
        List<String> alertIds = pendingAlertIds.value();
        if (alertIds == null) alertIds = new ArrayList<>();
        if (!alertIds.contains(alert.getAlertId())) {
            alertIds.add(alert.getAlertId());
            pendingAlertIds.update(alertIds);
        }

        // Register cleanup timer
        long cleanupTime = ctx.timerService().currentProcessingTime() + CROSS_WINDOW_MS;
        ctx.timerService().registerProcessingTimeTimer(cleanupTime);

        // Check if there's a recent metric anomaly for this node
        MetricEvent recentAnomaly = lastAnomalyMetric.value();
        if (recentAnomaly != null) {
            long age = alert.getTimestamp() - recentAnomaly.getTimestamp();
            if (Math.abs(age) <= CROSS_WINDOW_MS) {
                // Alert confirms the anomaly — forward the anomaly immediately
                recentAnomaly.setAnomalyCandidate(true);
                recentAnomaly.setAnomalyActive(
                        "cross-validated:" + alert.getAlertId());

                LOG.info("Alert triggers immediate anomaly forward: node={} alert={} metric={}",
                        alert.getNodeId(), alert.getAlertId(), recentAnomaly.getMetricType());

                // Mark alert as matched
                alert.setMatched(true);

                // Forward the confirmed anomaly — this effectively bypasses
                // the de-jitter window by re-emitting the anomaly event
                out.collect(recentAnomaly);
            }
        }
        // Note: AlertEvents themselves are not forwarded downstream —
        // they enrich the MetricEvent stream instead.
    }

    // ── Timer: cleanup stale state ─────────────────────────────

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricEvent> out)
            throws Exception {

        // Clear stale state after the cross-window expires
        MetricEvent anomaly = lastAnomalyMetric.value();
        AlertEvent alert = lastAlert.value();
        long now = ctx.timerService().currentProcessingTime();

        if (anomaly != null && (now - anomaly.getTimestamp()) > CROSS_WINDOW_MS) {
            lastAnomalyMetric.clear();
        }
        if (alert != null && (now - alert.getTimestamp()) > CROSS_WINDOW_MS) {
            lastAlert.clear();
        }
        // Clear accumulated alert IDs
        pendingAlertIds.clear();
    }
}
