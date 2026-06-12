package com.ifr.anomaly.process;

import com.ifr.anomaly.config.ThresholdConfig;
import com.ifr.anomaly.model.IncidentEvent;
import com.ifr.anomaly.model.MetricEvent;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Tumbling-window aggregation: groups confirmed anomalies by
 * (node_id) within a 5-minute window into a single IncidentEvent.
 *
 * This prevents "alert storms" where a single root cause triggers
 * dozens of separate webhook calls.
 */
public class AggregationFunction
        implements AggregateFunction<MetricEvent, AggregationFunction.Accumulator, IncidentEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AggregationFunction.class);

    // ------------------------------------------------------------------
    // Accumulator
    // ------------------------------------------------------------------

    public static class Accumulator {
        public String nodeId;
        public String lineProfile;
        public long firstTriggerTime = Long.MAX_VALUE;
        public long lastTriggerTime = Long.MIN_VALUE;
        public int alertCount;
        public String severityMax = "warning";
        public IncidentEvent.AggregatedAlert worstAlert;
        public double worstSigma = 0.0;
        public String metricGroup = "mixed";
        public List<String> externalAlertIds = new java.util.ArrayList<>();
        public boolean crossValidated = false;
    }

    // ------------------------------------------------------------------
    // AggregateFunction impl
    // ------------------------------------------------------------------

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(MetricEvent event, Accumulator acc) {
        if (acc.nodeId == null) {
            acc.nodeId = event.getNodeId();
            acc.lineProfile = event.getLineProfile();
        }

        acc.firstTriggerTime = Math.min(acc.firstTriggerTime, event.getTimestamp());
        acc.lastTriggerTime = Math.max(acc.lastTriggerTime, event.getTimestamp());
        acc.alertCount++;

        // Track worst severity
        acc.severityMax = maxSeverity(acc.severityMax, event.getSeverity());

        // Track worst deviation
        if (event.getDeviationSigma() > acc.worstSigma) {
            acc.worstSigma = event.getDeviationSigma();
            acc.worstAlert = new IncidentEvent.AggregatedAlert(
                    event.getNodeId() + "-" + event.getMetricType(),
                    event.getNodeId(),
                    event.getNodeType(),
                    event.getMetricType(),
                    event.getMetricValue(),
                    event.getBaselineMean(),
                    event.getBaselineStd(),
                    event.getDeviationSigma(),
                    event.getSeverity(),
                    null,  // tags passed as null (simplified for Flink serialization)
                    event.getTimestamp(),
                    event.getMetricValue()
            );
            acc.metricGroup = classifyMetricGroup(event.getMetricType());
        }

        // Collect external alert IDs from cross-validation
        String anomalyActive = event.getAnomalyActive();
        if (anomalyActive != null && anomalyActive.startsWith("cross-validated:")) {
            acc.crossValidated = true;
            String idsPart = anomalyActive.substring("cross-validated:".length());
            for (String id : idsPart.split(",")) {
                id = id.trim();
                if (!id.isEmpty() && !acc.externalAlertIds.contains(id)) {
                    acc.externalAlertIds.add(id);
                }
            }
        }
        // Also check for alerts: prefix
        if (anomalyActive != null && anomalyActive.contains("alerts:")) {
            int idx = anomalyActive.indexOf("alerts:");
            String idsPart = anomalyActive.substring(idx + "alerts:".length());
            for (String id : idsPart.split(",")) {
                id = id.trim();
                if (!id.isEmpty() && !acc.externalAlertIds.contains(id)) {
                    acc.externalAlertIds.add(id);
                }
            }
        }

        return acc;
    }

    @Override
    public IncidentEvent getResult(Accumulator acc) {
        IncidentEvent incident = new IncidentEvent();
        incident.setIncidentId("inc-" + acc.firstTriggerTime + "-" + acc.nodeId.hashCode());
        incident.setTriggerTime(new java.util.Date(acc.firstTriggerTime).toInstant().toString());
        incident.setAggregationWindowSeconds(ThresholdConfig.AGGREGATION_WINDOW_SECONDS);
        incident.setAlertCount(acc.alertCount);
        incident.setSeverityMax(acc.severityMax);
        incident.setAffectedLineProfile(acc.lineProfile);
        incident.setWorstAlert(acc.worstAlert);
        incident.setNodeId(acc.nodeId);
        incident.setMetricGroup(acc.metricGroup);
        incident.setChangeoverActive(false); // overridden if needed
        incident.setExternalAlertIds(acc.externalAlertIds.isEmpty() ? null : acc.externalAlertIds);
        incident.setCrossValidated(acc.crossValidated);

        // Priority score: severity × sigma_factor (+ boost if cross-validated)
        double sevWeight = severityWeight(acc.severityMax);
        double score = sevWeight * (1.0 + acc.worstSigma / 3.0);
        if (acc.crossValidated) {
            score *= 1.5;  // 50% boost when cross-validated by external alert
        }
        incident.setPriorityScore(score);

        LOG.info("Aggregated incident: id={} node={} alerts={} worstMetric={} sigma={:.2f}",
                incident.getIncidentId(), acc.nodeId, acc.alertCount,
                acc.worstAlert != null ? acc.worstAlert.getMetricType() : "?",
                acc.worstSigma);

        return incident;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator merged = new Accumulator();
        merged.nodeId = a.nodeId != null ? a.nodeId : b.nodeId;
        merged.lineProfile = a.lineProfile != null ? a.lineProfile : b.lineProfile;
        merged.firstTriggerTime = Math.min(a.firstTriggerTime, b.firstTriggerTime);
        merged.lastTriggerTime = Math.max(a.lastTriggerTime, b.lastTriggerTime);
        merged.alertCount = a.alertCount + b.alertCount;
        merged.severityMax = maxSeverity(a.severityMax, b.severityMax);
        merged.crossValidated = a.crossValidated || b.crossValidated;
        merged.externalAlertIds = new java.util.ArrayList<>(a.externalAlertIds);
        for (String id : b.externalAlertIds) {
            if (!merged.externalAlertIds.contains(id)) {
                merged.externalAlertIds.add(id);
            }
        }

        if (b.worstSigma > a.worstSigma) {
            merged.worstSigma = b.worstSigma;
            merged.worstAlert = b.worstAlert;
            merged.metricGroup = b.metricGroup;
        } else {
            merged.worstSigma = a.worstSigma;
            merged.worstAlert = a.worstAlert;
            merged.metricGroup = a.metricGroup;
        }
        return merged;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String maxSeverity(String a, String b) {
        int wa = severityWeight(a);
        int wb = severityWeight(b);
        return wb > wa ? b : a;
    }

    private static int severityWeight(String s) {
        if (s == null) return 0;
        switch (s) {
            case "critical": return 100;
            case "major":    return 60;
            case "minor":    return 20;
            default:         return 5;
        }
    }

    private static String classifyMetricGroup(String metricType) {
        if (metricType == null) return "unknown";
        if (metricType.contains("latency") || metricType.contains("delay")
                || metricType.contains("cycle_time") || metricType.contains("gc_pause"))
            return "latency";
        if (metricType.contains("queue") || metricType.contains("connection")
                || metricType.contains("pool"))
            return "queue";
        if (metricType.contains("error"))
            return "error";
        return "resource";
    }
}
