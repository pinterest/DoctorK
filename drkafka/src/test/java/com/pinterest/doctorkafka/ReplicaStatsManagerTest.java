package com.pinterest.doctorkafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ReplicaStatsManagerTest {
  private static final String ZKURL = "zk001/cluster1";
  private static final String TOPIC = "nginx_log";
  private List<ReplicaStat> replicaStats = new ArrayList<>();

  private void initialize() {
    ReplicaStat stats1 = new ReplicaStat(1502951705179L,
        TOPIC, 21, true, true, true, 9481888L, 9686567L, 9838856L, 34313663L, 31282690L,
        22680562L, 154167421996L, 154253138423L, 8.937965169074621, 72999272025L, 68  );

    ReplicaStat stats2 = new ReplicaStat(1502951705179L, TOPIC, 51, true, true, false,
       9539926L, 9745859L, 9899080L, 34523697L, 31474171L, 22819389L, 154167145733L,
        154253367479L, 8.992674338022038, 73446100348L, 69);

    replicaStats.add(stats1);
    replicaStats.add(stats2);
  }

  @Test
  public void updateReplicaReassignmentTimestampTest() throws Exception {
    initialize();
    ReplicaStatsManager.updateReplicaReassignmentTimestamp(ZKURL, replicaStats.get(0));
    ReplicaStatsManager.updateReplicaReassignmentTimestamp(ZKURL, replicaStats.get(1));

    TopicPartition topicPartition = new TopicPartition(TOPIC, 21);

    assertEquals(ReplicaStatsManager.replicaReassignmentTimestamps.get(ZKURL).size(), 2);
    assertEquals(
        (long)ReplicaStatsManager.replicaReassignmentTimestamps.get(ZKURL).get(topicPartition),
        1502951705179L);
  }
}
