package com.ifr.anomaly.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a single metric data point arriving from Kafka.
 *
 * Field contract matches the output of the Fake Data Generator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private long timestamp;          // epoch milliseconds

    @JsonProperty("node_id")
    private String nodeId;

    @JsonProperty("node_type")
    private String nodeType;

    @JsonProperty("line_profile")
    private String lineProfile;

    @JsonProperty("metric_type")
    private String metricType;

    @JsonProperty("metric_value")
    private double metricValue;

    @JsonProperty("unit")
    private String unit;

    @JsonProperty("severity")
    private String severity;

    @com.fasterxml.jackson.annotation.JsonIgnore
    private String tagsJson;  // Not used by Flink pipeline, skipped for serialisation

    @JsonProperty("batch_id")
    private String batchId;

    // ---- Derived fields (computed by pipeline) ----

    /** Composite key: nodeId + ":" + metricType */
    private String compositeKey;

    /** Baseline mean computed from 7-day history */
    private double baselineMean;

    /** Baseline standard deviation */
    private double baselineStd;

    /** Deviation in sigma units */
    private double deviationSigma;

    /** Set to true for anomaly candidates after z-score check */
    private boolean anomalyCandidate;

    /** Anomaly scenario name (if active during injection) */
    @JsonProperty("anomaly_active")
    private String anomalyActive;

    // ---- Getters / Setters ----

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getNodeType() { return nodeType; }
    public void setNodeType(String nodeType) { this.nodeType = nodeType; }

    public String getLineProfile() { return lineProfile; }
    public void setLineProfile(String lineProfile) { this.lineProfile = lineProfile; }

    public String getMetricType() { return metricType; }
    public void setMetricType(String metricType) { this.metricType = metricType; }

    public double getMetricValue() { return metricValue; }
    public void setMetricValue(double metricValue) { this.metricValue = metricValue; }

    public String getUnit() { return unit; }
    public void setUnit(String unit) { this.unit = unit; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getTagsJson() { return tagsJson; }
    public void setTagsJson(String tagsJson) { this.tagsJson = tagsJson; }

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public String getCompositeKey() { return compositeKey; }
    public void setCompositeKey(String compositeKey) { this.compositeKey = compositeKey; }

    public double getBaselineMean() { return baselineMean; }
    public void setBaselineMean(double baselineMean) { this.baselineMean = baselineMean; }

    public double getBaselineStd() { return baselineStd; }
    public void setBaselineStd(double baselineStd) { this.baselineStd = baselineStd; }

    public double getDeviationSigma() { return deviationSigma; }
    public void setDeviationSigma(double deviationSigma) { this.deviationSigma = deviationSigma; }

    public boolean isAnomalyCandidate() { return anomalyCandidate; }
    public void setAnomalyCandidate(boolean anomalyCandidate) { this.anomalyCandidate = anomalyCandidate; }

    public String getAnomalyActive() { return anomalyActive; }
    public void setAnomalyActive(String anomalyActive) { this.anomalyActive = anomalyActive; }

    /**
     * Derive composite key from nodeId and metricType.
     */
    public void buildCompositeKey() {
        this.compositeKey = this.nodeId + ":" + this.metricType;
    }
}
