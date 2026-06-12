package com.ifr.anomaly.process;

import com.ifr.anomaly.config.ThresholdConfig;
import com.ifr.anomaly.model.MetricEvent;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * KeyedProcessFunction that maintains a 7-day rolling baseline per
 * (node_id, metric_type) key using RocksDB MapState.
 *
 * State layout:
 *   MapState<bucketKey, BucketStats>  — up to 2016 buckets per key
 *
 * BucketKey = epochSecond / 300   (5-minute bucket)
 *
 * For each incoming metric:
 *   1. Update the current bucket's running stats (Welford single-pass)
 *   2. Compute the historical mean & std from the same bucket offset
 *      over the past 7 days (lookup bucketKey - N*288)
 *   3. Emit a MetricEvent enriched with baselineMean, baselineStd,
 *      deviationSigma.
 */
public class DynamicBaselineFunction
        extends KeyedProcessFunction<String, MetricEvent, MetricEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicBaselineFunction.class);

    /** Number of 5-min buckets per day = 24*60/5 = 288 */
    private static final long BUCKETS_PER_DAY = 288;

    private transient MapState<Long, BucketStats> bucketState;

    // ------------------------------------------------------------------
    // Bucket statistics (Welford online algorithm)
    // ------------------------------------------------------------------

    public static class BucketStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public long count;
        public double mean;
        public double m2; // sum of squared differences from mean

        public BucketStats() {}

        /** Add a single value, updating mean and variance online. */
        public void add(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
        }

        public double variance() {
            return count > 1 ? m2 / (count - 1) : 0.0;
        }

        public double std() {
            return Math.sqrt(variance());
        }
    }

    // ------------------------------------------------------------------
    // Open
    // ------------------------------------------------------------------

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.milliseconds(ThresholdConfig.STATE_TTL_MS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000) // compact 1000 entries at a time
                .build();

        MapStateDescriptor<Long, BucketStats> descriptor =
                new MapStateDescriptor<>("baseline-buckets", Long.class, BucketStats.class);
        descriptor.enableTimeToLive(ttlConfig);

        bucketState = getRuntimeContext().getMapState(descriptor);
    }

    // ------------------------------------------------------------------
    // Process
    // ------------------------------------------------------------------

    @Override
    public void processElement(MetricEvent event,
                               KeyedProcessFunction<String, MetricEvent, MetricEvent>.Context ctx,
                               Collector<MetricEvent> out) throws Exception {

        // 1. Build composite key
        event.buildCompositeKey();

        // 2. Compute current bucket
        long bucketKey = ThresholdConfig.computeBucketKey(event.getTimestamp());

        // 3. Update running stats for this bucket
        BucketStats stats = bucketState.get(bucketKey);
        if (stats == null) {
            stats = new BucketStats();
        }
        stats.add(event.getMetricValue());
        bucketState.put(bucketKey, stats);

        // 4. Build baseline from historical same-bucket offsets
        double sumMean = 0.0;
        int validDays = 0;

        for (int day = 1; day <= ThresholdConfig.BASELINE_DAYS; day++) {
            long histKey = bucketKey - day * BUCKETS_PER_DAY;
            BucketStats hist = bucketState.get(histKey);
            if (hist != null && hist.count > 0) {
                sumMean += hist.mean;
                validDays++;
            }
        }

        // 5. If insufficient history, skip anomaly check (cold start)
        if (validDays < 2) {
            event.setBaselineMean(event.getMetricValue());
            event.setBaselineStd(1.0);
            event.setDeviationSigma(0.0);
            event.setAnomalyCandidate(false);
            out.collect(event);
            return;
        }

        double baselineMean = sumMean / validDays;

        // Compute pooled std across historical buckets
        double pooledM2 = 0.0;
        long pooledCount = 0;
        for (int day = 1; day <= ThresholdConfig.BASELINE_DAYS; day++) {
            long histKey = bucketKey - day * BUCKETS_PER_DAY;
            BucketStats hist = bucketState.get(histKey);
            if (hist != null && hist.count > 0) {
                // Adjust m2 to reference the pooled mean
                double delta = hist.mean - baselineMean;
                pooledM2 += hist.m2 + hist.count * delta * delta;
                pooledCount += hist.count;
            }
        }
        double baselineStd = pooledCount > 1
                ? Math.sqrt(pooledM2 / (pooledCount - 1))
                : 1.0;

        // 6. Compute deviation
        double deviation = (baselineStd > 0)
                ? Math.abs(event.getMetricValue() - baselineMean) / baselineStd
                : 0.0;

        event.setBaselineMean(baselineMean);
        event.setBaselineStd(baselineStd);
        event.setDeviationSigma(deviation);

        // Anomaly check is done downstream by ZScoreRouter — we just enrich here
        event.setAnomalyCandidate(false);

        out.collect(event);
    }
}
