package com.ifr.anomaly.process;

import com.ifr.anomaly.config.ThresholdConfig;
import com.ifr.anomaly.model.MetricEvent;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes each MetricEvent to:
 *   - main output  (anomaly candidate — passed to de-jitter)
 *   - side output  (normal — dropped or logged)
 *
 * Uses differentiated thresholds:
 *   - For latency / queue / resource: z-score > threshold
 *   - For error_rate: absolute value > 1%
 */
public class ZScoreRouter extends ProcessFunction<MetricEvent, MetricEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ZScoreRouter.class);

    /** Side output tag for normal (non-anomalous) metrics. */
    public static final OutputTag<MetricEvent> NORMAL_METRICS =
            new OutputTag<MetricEvent>("normal-metrics") {};

    /** Side output tag for cold-start metrics (insufficient history). */
    public static final OutputTag<MetricEvent> COLD_START_METRICS =
            new OutputTag<MetricEvent>("cold-start-metrics") {};

    @Override
    public void processElement(MetricEvent event,
                               ProcessFunction<MetricEvent, MetricEvent>.Context ctx,
                               Collector<MetricEvent> out) throws Exception {

        String metricType = event.getMetricType();
        String lineProfile = event.getLineProfile() != null
                ? event.getLineProfile() : "general";

        boolean isAnomaly;

        if (ThresholdConfig.isAbsoluteThreshold(metricType)) {
            // Error rate: use absolute threshold
            isAnomaly = event.getMetricValue() > ThresholdConfig.ERROR_RATE_ABSOLUTE_THRESHOLD;
        } else if (event.getDeviationSigma() == 0.0 && event.getBaselineStd() <= 1.0) {
            // Cold start — insufficient history
            ctx.output(COLD_START_METRICS, event);
            return;
        } else {
            double threshold = ThresholdConfig.getThreshold(metricType, lineProfile);
            isAnomaly = event.getDeviationSigma() > threshold;
        }

        event.setAnomalyCandidate(isAnomaly);

        if (isAnomaly) {
            out.collect(event);
        } else {
            ctx.output(NORMAL_METRICS, event);
        }
    }
}
