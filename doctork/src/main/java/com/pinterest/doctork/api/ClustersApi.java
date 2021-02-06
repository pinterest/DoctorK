package com.pinterest.doctork.api;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.pinterest.doctork.DoctorK;

@Path("/clusters")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class ClustersApi extends DoctorKApi {

  public ClustersApi(DoctorK doctorK) {
    super(doctorK);
  }

  @GET
  public List<String> getClusterNames() {
    return getDoctorK().getClusterNames();
  }

}
