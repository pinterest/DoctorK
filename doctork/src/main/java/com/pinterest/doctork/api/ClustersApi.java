package com.pinterest.doctork.api;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.pinterest.doctork.KafkaClusterManager;
import com.pinterest.doctork.api.dto.ClusterDetail;
import com.pinterest.doctork.api.dto.ClusterSummary;

@Path("/clusters")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class ClustersApi extends DoctorKApi {

  public ClustersApi(com.pinterest.doctork.DoctorK doctorK) {
    super(doctorK);
  }

  @GET
  public List<ClusterSummary> getClusterInfo() {
    return getDoctorK().getClusterManagers().stream().filter(cm -> cm.getCluster() != null)
        .map(cm -> new ClusterSummary(cm.getClusterName(), cm.getClusterName(),
            cm.getCluster().getZkUrl(), 0, 0, cm.getCluster().getTopicPartitions().size(),
            cm.getCluster().getBrokers().size()))
        .collect(Collectors.toList());
  }

  @Path("/{clusterName}")
  @GET
  public ClusterDetail getClusterDetails(@PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = getDoctorK().getClusterManager(clusterName);
    String clusterId = clusterManager.getCluster().getZkUrl();

    return new ClusterDetail();
  }

}
