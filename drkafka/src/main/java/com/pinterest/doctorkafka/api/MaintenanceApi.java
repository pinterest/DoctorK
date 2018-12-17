package com.pinterest.doctorkafka.api;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.pinterest.doctorkafka.DoctorKafka;
import com.pinterest.doctorkafka.KafkaClusterManager;

@Path("/cluster/{clusterName}/admin/maintenance")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class MaintenanceApi {

  private static final Logger LOG = LogManager.getLogger(MaintenanceApi.class);
  private DoctorKafka drKafka;

  public MaintenanceApi(DoctorKafka drKafka) {
    this.drKafka = drKafka;
  }

  @GET
  public boolean checkMaintenance(@PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    return clusterManager.isMaintenanceModeEnabled();
  }

  @PUT
  public void enableMaintenance(@Context HttpServletRequest ctx,
      @PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    clusterManager.enableMaintenanceMode();
    LOG.info("Enabled maintenance mode for cluster:" + clusterName + " by user:"
        + ctx.getRemoteUser() + " from ip:" + ctx.getRemoteHost());
  }

  @DELETE
  public void disableMaintenance(@Context HttpServletRequest ctx,
      @PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    clusterManager.disableMaintenanceMode();
    LOG.info("Dsiabled maintenance mode for cluster:" + clusterName + " by user:"
        + ctx.getRemoteUser() + " from ip:" + ctx.getRemoteHost());
  }

  private KafkaClusterManager checkAndGetClusterManager(String clusterName) {
    KafkaClusterManager clusterManager = drKafka.getClusterManager(clusterName);
    if (clusterManager == null) {
      throw new NotFoundException("Unknown clustername:" + clusterName);
    }
    return clusterManager;
  }

}