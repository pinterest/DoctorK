package com.pinterest.doctorkafka.api;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.pinterest.doctorkafka.DoctorKafka;
import com.pinterest.doctorkafka.KafkaClusterManager;
import com.pinterest.doctorkafka.api.dto.ClusterDetail;
import com.pinterest.doctorkafka.api.dto.ClusterSummary;

@Path("/clusters")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class ClustersApi extends DoctorKafkaApi {

  public ClustersApi(DoctorKafka drKafka) {
    super(drKafka);
  }

  @GET
  public List<ClusterSummary> getClusterInfo() {
    return getDrkafka().getClusterManagers().stream().filter(cm -> cm.getCluster() != null)
        .map(cm -> new ClusterSummary(cm.getClusterName(), cm.getClusterName(),
            cm.getCluster().getZkUrl(), 0, 0, cm.getCluster().getTopicPartitions().size(),
            cm.getCluster().getBrokers().size()))
        .collect(Collectors.toList());
  }

  @Path("/{clusterName}")
  @GET
  public ClusterDetail getClusterDetails(@PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = getDrkafka().getClusterManager(clusterName);
    String clusterId = clusterManager.getCluster().getZkUrl();

    return new ClusterDetail();
  }

}
