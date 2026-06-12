package com.ifr.anomaly;

import com.ifr.anomaly.config.ThresholdConfig;
import com.ifr.anomaly.model.AlertEvent;
import com.ifr.anomaly.model.IncidentEvent;
import com.ifr.anomaly.model.MetricEvent;
import com.ifr.anomaly.process.AggregationFunction;
import com.ifr.anomaly.process.AlertCrossValidator;
import com.ifr.anomaly.process.DeJitterWindow;
import com.ifr.anomaly.process.DynamicBaselineFunction;
import com.ifr.anomaly.process.ZScoreRouter;
import com.ifr.anomaly.sink.WebhookSink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * ─── Industrial Fault Detection Flink Job ───
 *
 * Topology (dual-stream):
 *
 *   Kafka (industrial-metrics)              Kafka (industrial-alerts)
 *     → MetricDeserializer                    → AlertDeserializer
 *     → Watermark (5s)                        → Watermark (5s)
 *     → KeyBy(node_id + metric_type)          │
 *     → DynamicBaselineFunction               │
 *     → ZScoreRouter                          │
 *     → DeJitterWindow                        │
 *     └────── ConnectStream + KeyBy(node_id) ─┘
 *                      │
 *                      ▼
 *            AlertCrossValidator (cross-reference alerts ↔ anomalies)
 *                      │
 *                      ▼
 *            Aggregation (5-min tumbling window per node_id)
 *                      │
 *                      ▼
 *            WebhookSink → Supervisor
 *
 * Dual-stream benefits:
 *   - External alerts (Prometheus/Zabbix) cross-validate metric anomalies
 *   - Cross-validated incidents get 50% priority score boost
 *   - Alert IDs attached to IncidentEvent for Supervisor context
 *
 * Key design decisions (per Phase 2 correction):
 *   - Event-time processing with 5s watermark tolerance
 *   - RocksDB state backend for 7-day baseline (2016 buckets per key)
 *   - State TTL: 8 days (7 baseline + 1 cleanup delay)
 *   - De-jitter: N=3 consecutive anomaly windows required
 *   - Aggregation: 5-min tumbling window per node_id
 */
public class AnomalyDetectionJob {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJob.class);

    // ---- Configurable via env / Flink parameters ----
    private static final String KAFKA_BROKERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String METRICS_TOPIC =
            System.getenv().getOrDefault("TOPIC_METRICS", "industrial-metrics");
    private static final String ALERTS_TOPIC =
            System.getenv().getOrDefault("TOPIC_ALERTS", "industrial-alerts");
    private static final String CONSUMER_GROUP =
            System.getenv().getOrDefault("KAFKA_CONSUMER_GROUP", "flink-anomaly-detector");
    private static final String ALERTS_CONSUMER_GROUP =
            System.getenv().getOrDefault("KAFKA_ALERTS_CONSUMER_GROUP", "flink-alert-consumer");

    public static void main(String[] args) throws Exception {

        // ── 1. Environment ──────────────────────────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint every 60s (exactly-once for production parity)
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);

        // RocksDB state backend (handles large MapState for baselines)
        env.setStateBackend(
                new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend(true));

        // ── 2. Kafka Source ─────────────────────────────────────────
        KafkaSource<MetricEvent> kafkaSource = KafkaSource.<MetricEvent>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(METRICS_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new MetricDeserializer())
                .setProperty("max.poll.records", "500")
                .setProperty("fetch.min.bytes", "1024")
                .build();

        DataStream<MetricEvent> metricStream = env
                .fromSource(kafkaSource,
                        WatermarkStrategy
                                .<MetricEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                                .withIdleness(Duration.ofSeconds(30)),
                        "Kafka-Metrics-Source")
                .name("kafka-metrics-source")
                .uid("kafka-metrics-source");

        // ── 3. Dynamic Baseline (KeyedProcessFunction) ──────────────
        SingleOutputStreamOperator<MetricEvent> baselined = metricStream
                .keyBy(MetricEvent::getCompositeKey)
                .process(new DynamicBaselineFunction())
                .name("dynamic-baseline")
                .uid("dynamic-baseline");

        // ── 4. Z-Score Router (differentiated thresholds) ───────────
        SingleOutputStreamOperator<MetricEvent> anomalyStream = baselined
                .process(new ZScoreRouter())
                .name("zscore-router")
                .uid("zscore-router");

        // Side output: normal metrics (can be logged or dropped in prod)
        DataStream<MetricEvent> normalStream = anomalyStream
                .getSideOutput(ZScoreRouter.NORMAL_METRICS);

        // ── 5. De-Jitter (persistence check) ────────────────────────
        SingleOutputStreamOperator<MetricEvent> confirmedAnomalies = anomalyStream
                .keyBy(MetricEvent::getCompositeKey)
                .process(new DeJitterWindow())
                .name("dejitter-window")
                .uid("dejitter-window");

        // ── 5b. Alert Kafka Source (independent alert stream) ────────
        KafkaSource<AlertEvent> alertKafkaSource = KafkaSource.<AlertEvent>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(ALERTS_TOPIC)
                .setGroupId(ALERTS_CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new AlertDeserializer())
                .setProperty("max.poll.records", "200")
                .build();

        DataStream<AlertEvent> alertStream = env
                .fromSource(alertKafkaSource,
                        WatermarkStrategy
                                .<AlertEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                                .withIdleness(Duration.ofSeconds(30)),
                        "Kafka-Alerts-Source")
                .name("kafka-alerts-source")
                .uid("kafka-alerts-source");

        // ── 6. Cross-Validate: connect anomaly stream with alert stream ─
        ConnectedStreams<MetricEvent, AlertEvent> connected =
                confirmedAnomalies.connect(alertStream);

        SingleOutputStreamOperator<MetricEvent> enrichedAnomalies = connected
                .keyBy(MetricEvent::getNodeId, AlertEvent::getNodeId)
                .process(new AlertCrossValidator())
                .name("alert-cross-validator")
                .uid("alert-cross-validator");

        // ── 7. Incident Aggregation (5-min tumbling window per node) ─
        DataStream<IncidentEvent> incidents = enrichedAnomalies
                .keyBy(MetricEvent::getNodeId)
                .window(TumblingEventTimeWindows.of(
                        Time.seconds(ThresholdConfig.AGGREGATION_WINDOW_SECONDS)))
                .aggregate(new AggregationFunction())
                .name("incident-aggregation")
                .uid("incident-aggregation");

        // ── 8. Webhook Sink → Supervisor ────────────────────────────
        incidents.addSink(new WebhookSink())
                .name("webhook-sink")
                .uid("webhook-sink");

        // ── 9. (Dev) Log normal/cold-start throughput ───────────────
        normalStream
                .map(e -> {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Normal metric: {}={}", e.getCompositeKey(), e.getMetricValue());
                    }
                    return e;
                })
                .name("normal-metrics-logger")
                .uid("normal-metrics-logger");

        // ── 10. Execute ──────────────────────────────────────────────
        LOG.info("=== Industrial Fault Detection Job Starting (Dual-Stream) ===");
        LOG.info("Metrics Kafka: {}, Topic: {}", KAFKA_BROKERS, METRICS_TOPIC);
        LOG.info("Alerts Kafka:  {}, Topic: {}", KAFKA_BROKERS, ALERTS_TOPIC);
        LOG.info("Checkpoint: 60s | Watermark: 5s out-of-order | State: RocksDB");
        LOG.info("De-jitter: {} consecutive windows | Aggregation: {}s",
                ThresholdConfig.DEJITTER_CONSECUTIVE_WINDOWS,
                ThresholdConfig.AGGREGATION_WINDOW_SECONDS);

        env.execute("Industrial Fault Detection — Anomaly Pipeline");
    }

    // ─────────────────────────────────────────────────────────────────
    // Kafka Deserializer (Flink 1.18 compatible)
    // ─────────────────────────────────────────────────────────────────

    public static class MetricDeserializer
            implements KafkaRecordDeserializationSchema<MetricEvent> {

        private static final long serialVersionUID = 1L;
        private transient com.fasterxml.jackson.databind.ObjectMapper mapper;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MetricEvent> out)
                throws IOException {
            if (mapper == null) {
                mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            }
            try {
                String json = new String(record.value(), StandardCharsets.UTF_8);
                MetricEvent event = mapper.readValue(json, MetricEvent.class);
                event.buildCompositeKey();
                out.collect(event);
            } catch (Exception e) {
                LOG.error("Failed to deserialize metric: {} bytes — {}",
                    record.value().length, e.getMessage());
            }
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<MetricEvent> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(MetricEvent.class);
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Alert Kafka Deserializer
    // ─────────────────────────────────────────────────────────────────

    public static class AlertDeserializer
            implements KafkaRecordDeserializationSchema<AlertEvent> {

        private static final long serialVersionUID = 1L;
        private transient com.fasterxml.jackson.databind.ObjectMapper mapper;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<AlertEvent> out)
                throws IOException {
            if (mapper == null) {
                mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            }
            try {
                String json = new String(record.value(), StandardCharsets.UTF_8);
                AlertEvent event = mapper.readValue(json, AlertEvent.class);
                out.collect(event);
            } catch (Exception e) {
                LOG.error("Failed to deserialize alert: {} bytes — {}",
                    record.value().length, e.getMessage());
            }
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<AlertEvent> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInformation.of(AlertEvent.class);
        }
    }
}
