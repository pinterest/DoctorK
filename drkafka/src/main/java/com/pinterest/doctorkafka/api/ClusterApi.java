package com.pinterest.doctorkafka.api;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.pinterest.doctorkafka.DoctorKafka;

@Path("/cluster")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class ClusterApi {

  private DoctorKafka drKafka;

  public ClusterApi(DoctorKafka drKafka) {
    this.drKafka = drKafka;
  }

  @GET
  public List<String> getClusterNames() {
    return drKafka.getClusterNames();
  }

}
