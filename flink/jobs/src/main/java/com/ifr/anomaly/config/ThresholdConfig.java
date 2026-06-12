package com.ifr.anomaly.config;

import java.io.Serializable;
import java.util.*;

/**
 * Differentiated z-score thresholds per metric type group.
 *
 * Thresholds follow the design decisions from Phase 1:
 *   - latency   → 2.5σ  (more sensitive to tail latency)
 *   - queue     → 2.0σ  (aggressive; queues can explode exponentially)
 *   - resource  → 3.0σ  (stable baselines)
 *   - error     → absolute threshold (z-score not meaningful near zero)
 *   - default   → 3.0σ
 */
public class ThresholdConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ---- Metric type → threshold group ----
    private static final Map<String, String> GROUP_MAP = new LinkedHashMap<>();

    static {
        // Latency-sensitive
        putAll("latency",
            "p99_latency_ms", "comms_latency_ms", "scan_cycle_ms",
            "cycle_time_ms", "gc_pause_ms", "program_cycle_us",
            "joint_angle_0_deg", "feed_rate_mm_min");

        // Queue / backlog
        putAll("queue",
            "queue_depth", "msg_queue_depth", "connection_pool_used",
            "thread_pool_active", "connection_count");

        // Resource utilisation (stable)
        putAll("resource",
            "cpu_usage", "mem_usage", "disk_usage", "disk_io_mbps",
            "disk_io_kbps", "net_in_mbps", "net_out_mbps",
            "battery_pct", "power_consumption_w",
            "temperature_c", "motor_temp_c");

        // Error rates → absolute thresholds (handled separately)
        putAll("error",
            "error_rate");
    }

    // ---- Group → sigma threshold ----
    private static final Map<String, Double> SIGMA_THRESHOLDS = new LinkedHashMap<>();

    static {
        SIGMA_THRESHOLDS.put("latency", 2.5);
        SIGMA_THRESHOLDS.put("queue", 2.0);
        SIGMA_THRESHOLDS.put("resource", 3.0);
        SIGMA_THRESHOLDS.put("error", Double.NaN); // use absolute
        SIGMA_THRESHOLDS.put("default", 3.0);
    }

    /** Absolute error-rate threshold (fraction: 0.01 → 1%). */
    public static final double ERROR_RATE_ABSOLUTE_THRESHOLD = 0.01;

    // ---- De-jitter config ----
    public static final int DEJITTER_CONSECUTIVE_WINDOWS = 3;
    public static final long DEJITTER_WINDOW_SIZE_MS = 30_000;   // 30 s
    public static final long DEJITTER_SLIDE_MS = 10_000;          // 10 s

    // ---- Aggregation config ----
    public static final long AGGREGATION_WINDOW_SECONDS = 300;    // 5 min

    // ---- Baseline config ----
    public static final int BASELINE_DAYS = 7;
    public static final long BUCKET_SIZE_SECONDS = 300;           // 5 min
    public static final long STATE_TTL_MS = 8L * 24 * 3600 * 1000; // 8 days

    // ---- Line-profile sigma offsets (added to base threshold) ----
    private static final Map<String, Double> LINE_PROFILE_OFFSETS = new LinkedHashMap<>();

    static {
        // precision_machining: more sensitive → lower threshold = subtract from sigma
        LINE_PROFILE_OFFSETS.put("precision_machining", -0.3);
        LINE_PROFILE_OFFSETS.put("packaging", 0.0);
        LINE_PROFILE_OFFSETS.put("assembly", -0.1);
        LINE_PROFILE_OFFSETS.put("general", 0.0);
    }

    // ---- Public API ----

    /**
     * Return the effective sigma threshold for a metric, accounting for
     * metric type and line profile.
     */
    public static double getThreshold(String metricType, String lineProfile) {
        String group = GROUP_MAP.getOrDefault(metricType, "default");
        double base = SIGMA_THRESHOLDS.getOrDefault(group, 3.0);

        if (Double.isNaN(base)) {
            return Double.NaN; // error rate — caller should check absolute
        }

        double offset = LINE_PROFILE_OFFSETS.getOrDefault(lineProfile, 0.0);
        return base + offset;
    }

    /** Check whether this metric type uses an absolute threshold. */
    public static boolean isAbsoluteThreshold(String metricType) {
        return "error".equals(GROUP_MAP.get(metricType));
    }

    public static long computeBucketKey(long timestampMs) {
        return timestampMs / (BUCKET_SIZE_SECONDS * 1000);
    }

    // ---- helpers ----

    private static void putAll(String group, String... metricTypes) {
        for (String mt : metricTypes) {
            GROUP_MAP.put(mt, group);
        }
    }
}
