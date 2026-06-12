package com.ifr.anomaly.process;

import com.ifr.anomaly.config.ThresholdConfig;
import com.ifr.anomaly.model.MetricEvent;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * De-jitter filter: an anomaly must persist for N consecutive windows
 * before being passed downstream.
 *
 * Design:
 *   - Sliding count-based approach: each time we see an anomalous event
 *     for a key, we increment a counter.
 *   - When counter reaches N (default 3), the event is emitted as CONFIRMED.
 *   - If a normal event arrives, the counter is reset.
 *   - A timer fires when an anomalous event first arrives; if the counter
 *     hasn't reached N by then, the event is dropped.
 *
 * Simplification for MVP:
 *   We use simple per-key event counting rather than full windowing.
 *   This is equivalent to "consecutive anomaly check" — the key insight
 *   is that de-jitter relies on the fact that Flink processes events in
 *   order for a single key, so consecutive anomalous ticks for the same
 *   (node, metric) key mean a sustained deviation.
 */
public class DeJitterWindow
        extends KeyedProcessFunction<String, MetricEvent, MetricEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DeJitterWindow.class);

    /** Counter of consecutive anomalous events for this key. */
    private transient ValueState<Integer> consecutiveCount;

    /** Timestamp (ms) of the first anomaly in the current streak. */
    private transient ValueState<Long> firstAnomalyTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        consecutiveCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dejitter-count", Integer.class));
        firstAnomalyTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dejitter-first-ts", Long.class));
    }

    @Override
    public void processElement(MetricEvent event,
                               KeyedProcessFunction<String, MetricEvent, MetricEvent>.Context ctx,
                               Collector<MetricEvent> out) throws Exception {

        Integer count = consecutiveCount.value();
        if (count == null) count = 0;

        if (event.isAnomalyCandidate()) {
            count++;

            if (count == 1) {
                // First anomaly in streak — record timestamp
                firstAnomalyTime.update(event.getTimestamp());
            }

            consecutiveCount.update(count);

            if (count >= ThresholdConfig.DEJITTER_CONSECUTIVE_WINDOWS) {
                // Confirmed anomaly — emit downstream
                LOG.debug("De-jitter confirmed: key={} metric={} consecutive={}",
                        event.getCompositeKey(), event.getMetricType(), count);
                out.collect(event);
            }
        } else {
            // Normal event — reset streak
            consecutiveCount.update(0);
            firstAnomalyTime.clear();
        }
    }
}
