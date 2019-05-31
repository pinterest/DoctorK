package com.pinterest.doctorkafka.api;

import com.pinterest.doctorkafka.DoctorKafka;

import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/clusters")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class ClustersApi extends DoctorKafkaApi {

  public ClustersApi(DoctorKafka drKafka) {
    super(drKafka);
  }

  @GET
  public List<String> getClusterNames() {
    return getDrkafka().getClusterNames();
  }

}
