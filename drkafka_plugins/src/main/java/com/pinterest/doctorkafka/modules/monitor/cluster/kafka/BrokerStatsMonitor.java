package com.pinterest.doctorkafka.modules.monitor.cluster.kafka;

import com.pinterest.doctorkafka.BrokerStats;
import com.pinterest.doctorkafka.DoctorKafkaMetrics;
import com.pinterest.doctorkafka.KafkaCluster;
import com.pinterest.doctorkafka.modules.context.state.cluster.kafka.KafkaState;
import com.pinterest.doctorkafka.modules.errors.ModuleConfigurationException;
import com.pinterest.doctorkafka.util.KafkaUtils;
import com.pinterest.doctorkafka.util.OpenTsdbMetricConverter;
import com.pinterest.doctorkafka.util.OperatorUtil;
import com.pinterest.doctorkafka.util.ReplicaStatsUtil;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This monitor ingests BrokerStats sent from the KafkaStats agent on each broker. The stats are sent
 * through a Kafka topic. The monitor first backfills from the topic to reconstruct historical stats,
 * then ingests the messages after the historical stats have been rebuilt. This implementation is meant
 * to ingest from a single topic containing all BrokerStats from every broker in each cluster DoctorKafka
 * is managing. We use a singleton ingestion instance to handle the stats from all clusters to save traffic volume.
 *
 * config:
 * [required]
 *   topic: <Topic containing BrokerStats sent from brokers>
 *   zkurl: <zookeeper connect string to the topic we are consuming of>
 *   backfill_seconds: <number of seconds to backfill>
 *   network:
 *     inbound_limit_mb: <bandwidth limit of the inbound traffic for brokers on this cluster>
 *     outbound_limit_mb <bandwidth limit of the outbound traffic for brokers on this cluster>
 * [optional]
 *   consumer_config: <properties used to initialize stat ingestion kafka consumer>
 *   sliding_window_size: <number of brokerstats to keep per TopicPartition for metrics aggregation>
 *
 */
public class BrokerStatsMonitor extends KafkaMonitor {
  private static final Logger LOG = LogManager.getLogger(BrokerStatsMonitor.class);

  private static final String CONFIG_BROKERSTATS_TOPIC_KEY = "topic";
  private static final String CONFIG_ZKURL_KEY = "zkurl";
  private static final String CONFIG_BACKFILL_WINDOW_SECONDS = "backfill_seconds";
  private static final String CONFIG_CONSUMER_CONFIG_KEY = "consumer_config";
  private static final String CONSUMER_CONFIG_SECURITY_PROTOCOL_KEY = "security.protocol";
  private static final String CONFIG_MB_IN_PER_SECOND_LIMIT_KEY = "network.inbound_limit_mb";
  private static final String CONFIG_MB_OUT_PER_SECOND_LIMIT_KEY = "network.outbound_limit_mb";
  private static final String CONFIG_SLIDING_WINDOW_SIZE_KEY = "sliding_window_size";

  private static final ConcurrentMap<String, KafkaCluster> clusters = new ConcurrentHashMap<>();
  private static volatile boolean initialized = false;
  private static volatile boolean isBackfillComplete = false;

  /*
   * kafkastats agent sends 1 brokerstat message every minute.
   * 1 days worth of brokerstats = 24 * 60 = 1440 data points.
   */
  private static final int DEFAULT_SLIDING_WINDOW_SIZE = 1440;

  private boolean isNetworkBandwidthSet = false;

  private double configBytesInPerSecondLimit;
  private double configBytesOutPerSecondLimit;

  // brokerstats backfill and collection thread that reconstructs the clusters
  private static class BrokerStatsCollector implements Runnable {
    private List<Processor> processors = new ArrayList<>();
    private String brokerStatsTopic;
    private String zkUrl;
    private int backfillWindowSeconds;
    private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
    private Properties consumerConfigs;
    private int slidingWindowSize;

    public BrokerStatsCollector(String brokerStatsTopic, String zkUrl, int backfillWindowSeconds, Properties consumerConfigs, int slidingWindowSize) {
      this.brokerStatsTopic = brokerStatsTopic;
      this.zkUrl = zkUrl;
      this.backfillWindowSeconds = backfillWindowSeconds;
      this.slidingWindowSize = slidingWindowSize;
      this.consumerConfigs = consumerConfigs;
      if (consumerConfigs.containsKey(CONSUMER_CONFIG_SECURITY_PROTOCOL_KEY)) {
        this.securityProtocol = SecurityProtocol.valueOf(consumerConfigs.getProperty(CONSUMER_CONFIG_SECURITY_PROTOCOL_KEY));
      }
    }

    @Override
    public void run() {
      LOG.info("Start rebuilding the replica stats by reading the past 24 hours brokerstats");
      KafkaConsumer<?, ?> kafkaConsumer = KafkaUtils.getKafkaConsumer(zkUrl,
          KafkaUtils.BYTE_ARRAY_DESERIALIZER,
          KafkaUtils.BYTE_ARRAY_DESERIALIZER,
          1, securityProtocol, consumerConfigs);
      long startTimestampInMillis = System.currentTimeMillis() - backfillWindowSeconds * 1000L;
      Map<TopicPartition, Long> backfillStartOffsets = ReplicaStatsUtil
          .getProcessingStartOffsets(kafkaConsumer, brokerStatsTopic, startTimestampInMillis);

      kafkaConsumer.unsubscribe();
      kafkaConsumer.assign(backfillStartOffsets.keySet());

      // backfillEndOffsets is the map of latest offsets at launch of doctorkafka.
      // Backfill will finish at these offsets and the realtime consumer will pick up the stats from these offsets to avoid overlapping consumption.
      Map<TopicPartition, Long> backfillEndOffsets = kafkaConsumer.endOffsets(backfillStartOffsets.keySet());
      KafkaUtils.closeConsumer(zkUrl);

      for(TopicPartition tp : backfillEndOffsets.keySet()){
        Processor processor = new Processor(tp, backfillStartOffsets.get(tp), backfillEndOffsets.get(tp), zkUrl, securityProtocol, slidingWindowSize);
        processors.add(processor);
        processor.start();
      }

      for (Processor processor : processors){
        try {
          processor.join();
        } catch (Exception e){
          LOG.error("Processor failed to backfill historical data", e);
        }
      }
      isBackfillComplete = true;
      LOG.info("Finish rebuilding the replica stats");

      // Backfill complete, start long-running background processor

      processors.clear();
      Processor processor = new Processor(backfillEndOffsets, zkUrl, securityProtocol, consumerConfigs, slidingWindowSize);
      processors.add(processor);
      processor.start();
    }
  }

  /**
   * Process brokerstats from either a topic or a topic-partition
   */
  private static class Processor implements Runnable {
    private static final long BROKER_STATS_POLL_INTERVAL_MS = 200L;
    private static final String BROKERSTATS_CONSUMER_GROUP =
        "operator_brokerstats_group_" + OperatorUtil.getHostname();

    private Consumer<byte[], byte[]> consumer;
    private Thread thread;
    private volatile boolean stopped = false;

    private TopicPartition topicPartition;
    private long endOffset = -1L;
    private int slidingWindowSize;

    // long running processor
    public Processor(Map<TopicPartition, Long> partitionOffsets, String zkUrl, SecurityProtocol securityProtocol, Properties consumerConfigs, int slidingWindowSize) {
      Properties properties = OperatorUtil.createKafkaConsumerProperties(
          zkUrl, BROKERSTATS_CONSUMER_GROUP, securityProtocol, consumerConfigs);
      consumer = new KafkaConsumer<>(properties);

      // seek from the end offsets we left off during backfilling
      consumer.assign(partitionOffsets.keySet());
      for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()){
        consumer.seek(entry.getKey(), entry.getValue());
      }
      this.slidingWindowSize = slidingWindowSize;
    }

    // backfill processor
    public Processor(TopicPartition topicPartition, long startOffset, long endOffset, String zkUrl, SecurityProtocol securityProtocol, int slidingWindowSize){
      this.topicPartition = topicPartition;
      this.endOffset = endOffset;
      this.slidingWindowSize = slidingWindowSize;

      String brokers = KafkaUtils.getBrokers(zkUrl, securityProtocol);
      LOG.info("ZkUrl: {}, Brokers: {}", zkUrl, brokers);
      Properties properties = OperatorUtil.createKafkaConsumerProperties(
          zkUrl, BROKERSTATS_CONSUMER_GROUP + "_" + topicPartition, securityProtocol, null);
      properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
      properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576 * 4);

      consumer = new KafkaConsumer<>(properties);
      Set<TopicPartition> topicPartitions = new HashSet<>();
      topicPartitions.add(topicPartition);
      consumer.assign(topicPartitions);
      consumer.seek(topicPartition, startOffset);
    }

    public void start() {
      thread = new Thread(this);
      thread.start();
    }

    public void stop() {
      stopped = true;
    }

    public void join() throws InterruptedException {
      thread.join();
    }

    @Override
    public void run() {
      ConsumerRecords<byte[], byte[]> records;
      try {
        while(!stopped) {
          // if single topic partition
          if(topicPartition != null && consumer.position(topicPartition) >= endOffset) {
            break;
          }
          records = consumer.poll(BROKER_STATS_POLL_INTERVAL_MS);
          for (ConsumerRecord<byte[], byte[]> record : records){
            BrokerStats brokerStats = OperatorUtil.deserializeBrokerStats(record);
            if (brokerStats == null || brokerStats.getName() == null) {
              OpenTsdbMetricConverter.incr(DoctorKafkaMetrics.MESSAGE_DESERIALIZE_ERROR, 1);
              continue;
            } else {
              OpenTsdbMetricConverter.incr(DoctorKafkaMetrics.BROKERSTATS_MESSAGES, 1,
                  "zkUrl= " + brokerStats.getZkUrl());
            }
            this.update(brokerStats);
          }

        }
      } catch (Exception e) {
        LOG.error("Exception in processing brokerstats", e);
        System.exit(-1);
      } finally {
        consumer.close();
      }
    }

    protected void update(BrokerStats brokerStats) {
      String brokerZkUrl = brokerStats.getZkUrl();
      // ignore the brokerstats from clusters that are not enabled operation automation.
      if (brokerZkUrl == null) {
        return;
      }

      KafkaCluster cluster = clusters.computeIfAbsent(brokerZkUrl, url -> new KafkaCluster(url, slidingWindowSize));
      cluster.recordBrokerStats(brokerStats);
    }
  }

  @Override
  public void configure(AbstractConfiguration config) throws ModuleConfigurationException {
    super.configure(config);
    if(!initialized){
      synchronized (clusters) {
        if(!initialized){
          initialized = true;
          if(!config.containsKey(CONFIG_BROKERSTATS_TOPIC_KEY)){
            throw new ModuleConfigurationException("Missing config " + CONFIG_BROKERSTATS_TOPIC_KEY + " for module " + this.getClass());
          }
          String topic = config.getString(CONFIG_BROKERSTATS_TOPIC_KEY);

          if(!config.containsKey(CONFIG_ZKURL_KEY)){
            throw new ModuleConfigurationException("Missing config " + CONFIG_ZKURL_KEY + " for module " + this.getClass());
          }
          String zkUrl = config.getString(CONFIG_ZKURL_KEY);

          if(!config.containsKey(CONFIG_BACKFILL_WINDOW_SECONDS)){
            throw new ModuleConfigurationException("Missing config " + CONFIG_BACKFILL_WINDOW_SECONDS + " for module " + this.getClass());
          }
          int backfillWindowSeconds = config.getInt(CONFIG_BACKFILL_WINDOW_SECONDS);

          Properties consumerConfigs = new Properties();
          if(config.containsKey(CONFIG_CONSUMER_CONFIG_KEY)){
            String consumerConfigStr = config.getString(CONFIG_CONSUMER_CONFIG_KEY);
            try{
              consumerConfigs.load(new StringReader(consumerConfigStr));
            } catch (Exception e){
              throw new ModuleConfigurationException("Error while parsing properties of " + CONFIG_CONSUMER_CONFIG_KEY + " for module " + this.getClass(), e);
            }
          }

          int slidingWindowSize = config.getInt(CONFIG_SLIDING_WINDOW_SIZE_KEY, DEFAULT_SLIDING_WINDOW_SIZE);

          BrokerStatsCollector collector = new BrokerStatsCollector(topic, zkUrl, backfillWindowSeconds, consumerConfigs, slidingWindowSize);
          Thread thread = new Thread(collector);
          thread.run();
        }
      }
    }
    if(!config.containsKey(CONFIG_MB_IN_PER_SECOND_LIMIT_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_MB_IN_PER_SECOND_LIMIT_KEY + " for module " + this.getClass());
    }
    configBytesInPerSecondLimit = config.getDouble(CONFIG_MB_IN_PER_SECOND_LIMIT_KEY)*1024*1024;

    if(!config.containsKey(CONFIG_MB_OUT_PER_SECOND_LIMIT_KEY)){
      throw new ModuleConfigurationException("Missing config " + CONFIG_MB_OUT_PER_SECOND_LIMIT_KEY + " for module " + this.getClass());
    }
    configBytesOutPerSecondLimit = config.getDouble(CONFIG_MB_OUT_PER_SECOND_LIMIT_KEY)*1024*1024;
  }

  @Override
  public KafkaState observe(KafkaState state) throws Exception {
    if(isBackfillComplete){
      KafkaCluster cluster = clusters.get(state.getZkUrl());
      if(!isNetworkBandwidthSet){
        cluster.setBytesInPerSecLimit(configBytesInPerSecondLimit);
        cluster.setBytesOutPerSecLimit(configBytesOutPerSecondLimit);
        isNetworkBandwidthSet = true;
      }
      state.setKafkaCluster(cluster);
    } else {
      state.stopOperations();
    }
    return state;
  }
}
