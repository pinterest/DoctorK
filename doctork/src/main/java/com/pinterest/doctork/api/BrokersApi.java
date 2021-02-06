package com.pinterest.doctork.api;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.pinterest.doctork.DoctorK;
import com.pinterest.doctork.KafkaBroker;
import com.pinterest.doctork.KafkaClusterManager;

@Path("/clusters/{clusterName}/brokers")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class BrokersApi extends DoctorKApi {

    public BrokersApi(DoctorK doctorK){
        super(doctorK);
    }

    @GET
    public List<KafkaBroker> getBrokerList(@PathParam("clusterName") String clusterName) {
        KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
        return clusterManager.getAllBrokers();
    }

}
