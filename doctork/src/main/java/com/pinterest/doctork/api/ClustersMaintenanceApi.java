package com.pinterest.doctork.api;

import javax.annotation.security.RolesAllowed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.pinterest.doctork.DoctorK;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.pinterest.doctork.KafkaClusterManager;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.util.ApiUtils;

@Path("/clusters/{clusterName}/admin/maintenance")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class ClustersMaintenanceApi extends DoctorKApi {

  private static final Logger LOG = LogManager.getLogger(ClustersMaintenanceApi.class);

  public ClustersMaintenanceApi(DoctorK doctorK) {
    super(doctorK);
  }

  @GET
  public boolean checkMaintenance(@PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    return clusterManager.isMaintenanceModeEnabled();
  }

  @PUT
  @RolesAllowed({ DoctorKConfig.DOCTORK_ADMIN_ROLE})
  public void enableMaintenance(@Context HttpServletRequest ctx,
      @PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    clusterManager.enableMaintenanceMode();
    ApiUtils.logAPIAction(LOG, ctx, "Enabled maintenance mode for cluster:" + clusterName);
  }

  @DELETE
  @RolesAllowed({ DoctorKConfig.DOCTORK_ADMIN_ROLE})
  public void disableMaintenance(@Context HttpServletRequest ctx,
      @PathParam("clusterName") String clusterName) {
    KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
    clusterManager.disableMaintenanceMode();
    ApiUtils.logAPIAction(LOG, ctx, "Disabled maintenance mode for cluster:" + clusterName);
  }

}