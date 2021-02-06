package com.pinterest.doctork.api;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.common.TopicPartition;

import com.pinterest.doctork.KafkaClusterManager;
import com.pinterest.doctork.api.dto.Topic;
import com.pinterest.doctork.plugins.context.state.cluster.kafka.KafkaState;

@Path("/clusters/{clusterName}/topics")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class TopicsApi extends DoctorKApi {

  public TopicsApi(com.pinterest.doctork.DoctorK doctorK) {
    super(doctorK);
  }

  @GET
  public Set<Topic> getTopics(@PathParam("clusterName") String clusterName) {
    KafkaClusterManager cm = checkAndGetClusterManager(clusterName);
    KafkaState currentState = cm.getCurrentState();
    Map<String, Set<TopicPartition>> topicPartitions = currentState.getKafkaCluster()
        .getTopicPartitions();
    Set<Topic> collect = topicPartitions.entrySet().stream()
        .map(e -> new Topic(e.getKey(), clusterName, e.getValue().size(), 3, 0,
            currentState.getKafkaCluster().getMaxMBInFor(e.getKey()),
            currentState.getKafkaCluster().getMaxMBOutFor(e.getKey())))
        .collect(Collectors.toSet());
    return collect;
  }

}
