package com.ifr.anomaly.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregated incident event sent to the Supervisor via webhook.
 * Mirrors the OpenAPI IncidentEvent schema from Phase 2.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IncidentEvent implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("incident_id")
    private String incidentId;

    @JsonProperty("trigger_time")
    private String triggerTime;          // ISO-8601

    @JsonProperty("aggregation_window_seconds")
    private long aggregationWindowSeconds;

    @JsonProperty("alert_count")
    private int alertCount;

    @JsonProperty("severity_max")
    private String severityMax;

    @JsonProperty("affected_line_profile")
    private String affectedLineProfile;

    @JsonProperty("priority_score")
    private double priorityScore;

    @JsonProperty("changeover_active")
    private boolean changeoverActive;

    // Fields for Supervisor's domain routing
    @JsonProperty("node_id")
    private String nodeId;

    @JsonProperty("metric_group")
    private String metricGroup;

    @JsonProperty("aggregated_alerts")
    private List<AggregatedAlert> aggregatedAlerts;

    // The single worst alert (for aggregation simplicity)
    @JsonProperty("worst_alert")
    private AggregatedAlert worstAlert;

    // External alert IDs from the independent alert stream (cross-validation)
    @JsonProperty("external_alert_ids")
    private List<String> externalAlertIds;

    // Whether this incident was cross-validated by the alert stream
    @JsonProperty("cross_validated")
    private boolean crossValidated;

    // ---- Nested: AggregatedAlert ----

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AggregatedAlert implements Serializable {
        private static final long serialVersionUID = 1L;

        @JsonProperty("alert_id")
        private String alertId;

        @JsonProperty("node_id")
        private String nodeId;

        @JsonProperty("node_type")
        private String nodeType;

        @JsonProperty("metric_type")
        private String metricType;

        @JsonProperty("current_value")
        private double currentValue;

        @JsonProperty("baseline_mean")
        private double baselineMean;

        @JsonProperty("baseline_std")
        private double baselineStd;

        @JsonProperty("deviation_sigma")
        private double deviationSigma;

        @JsonProperty("severity")
        private String severity;

        @JsonProperty("tags")
        private Map<String, String> tags;

        @JsonProperty("first_trigger_time")
        private long firstTriggerTime;

        @JsonProperty("peak_value")
        private double peakValue;

        public AggregatedAlert() {}

        public AggregatedAlert(String alertId, String nodeId, String nodeType,
                               String metricType, double currentValue,
                               double baselineMean, double baselineStd,
                               double deviationSigma, String severity,
                               Map<String, String> tags,
                               long firstTriggerTime, double peakValue) {
            this.alertId = alertId;
            this.nodeId = nodeId;
            this.nodeType = nodeType;
            this.metricType = metricType;
            this.currentValue = currentValue;
            this.baselineMean = baselineMean;
            this.baselineStd = baselineStd;
            this.deviationSigma = deviationSigma;
            this.severity = severity;
            this.tags = tags;
            this.firstTriggerTime = firstTriggerTime;
            this.peakValue = peakValue;
        }

        // ---- Getters / Setters ----
        public String getAlertId() { return alertId; }
        public void setAlertId(String alertId) { this.alertId = alertId; }
        public String getNodeId() { return nodeId; }
        public void setNodeId(String nodeId) { this.nodeId = nodeId; }
        public String getNodeType() { return nodeType; }
        public void setNodeType(String nodeType) { this.nodeType = nodeType; }
        public String getMetricType() { return metricType; }
        public void setMetricType(String metricType) { this.metricType = metricType; }
        public double getCurrentValue() { return currentValue; }
        public void setCurrentValue(double currentValue) { this.currentValue = currentValue; }
        public double getBaselineMean() { return baselineMean; }
        public void setBaselineMean(double baselineMean) { this.baselineMean = baselineMean; }
        public double getBaselineStd() { return baselineStd; }
        public void setBaselineStd(double baselineStd) { this.baselineStd = baselineStd; }
        public double getDeviationSigma() { return deviationSigma; }
        public void setDeviationSigma(double deviationSigma) { this.deviationSigma = deviationSigma; }
        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }
        public Map<String, String> getTags() { return tags; }
        public void setTags(Map<String, String> tags) { this.tags = tags; }
        public long getFirstTriggerTime() { return firstTriggerTime; }
        public void setFirstTriggerTime(long firstTriggerTime) { this.firstTriggerTime = firstTriggerTime; }
        public double getPeakValue() { return peakValue; }
        public void setPeakValue(double peakValue) { this.peakValue = peakValue; }
    }

    // ---- IncidentEvent getters / setters ----

    public String getIncidentId() { return incidentId; }
    public void setIncidentId(String incidentId) { this.incidentId = incidentId; }
    public String getTriggerTime() { return triggerTime; }
    public void setTriggerTime(String triggerTime) { this.triggerTime = triggerTime; }
    public long getAggregationWindowSeconds() { return aggregationWindowSeconds; }
    public void setAggregationWindowSeconds(long seconds) { this.aggregationWindowSeconds = seconds; }
    public int getAlertCount() { return alertCount; }
    public void setAlertCount(int alertCount) { this.alertCount = alertCount; }
    public String getSeverityMax() { return severityMax; }
    public void setSeverityMax(String severityMax) { this.severityMax = severityMax; }
    public String getAffectedLineProfile() { return affectedLineProfile; }
    public void setAffectedLineProfile(String line) { this.affectedLineProfile = line; }
    public double getPriorityScore() { return priorityScore; }
    public void setPriorityScore(double score) { this.priorityScore = score; }
    public boolean isChangeoverActive() { return changeoverActive; }
    public void setChangeoverActive(boolean active) { this.changeoverActive = active; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public String getMetricGroup() { return metricGroup; }
    public void setMetricGroup(String group) { this.metricGroup = group; }
    public List<AggregatedAlert> getAggregatedAlerts() { return aggregatedAlerts; }
    public void setAggregatedAlerts(List<AggregatedAlert> alerts) { this.aggregatedAlerts = alerts; }
    public AggregatedAlert getWorstAlert() { return worstAlert; }
    public void setWorstAlert(AggregatedAlert alert) { this.worstAlert = alert; }
    public List<String> getExternalAlertIds() { return externalAlertIds; }
    public void setExternalAlertIds(List<String> ids) { this.externalAlertIds = ids; }
    public boolean isCrossValidated() { return crossValidated; }
    public void setCrossValidated(boolean crossValidated) { this.crossValidated = crossValidated; }

    /** Serialise to JSON string for webhook body. */
    public String toJson() {
        try {
            // Wrap in the Supervisor's expected format
            if (worstAlert != null && (aggregatedAlerts == null || aggregatedAlerts.isEmpty())) {
                aggregatedAlerts = Collections.singletonList(worstAlert);
            }
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize IncidentEvent", e);
        }
    }
}
