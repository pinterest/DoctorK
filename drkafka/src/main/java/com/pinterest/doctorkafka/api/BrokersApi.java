package com.pinterest.doctorkafka.api;

import com.pinterest.doctorkafka.DoctorKafka;
import com.pinterest.doctorkafka.KafkaBroker;
import com.pinterest.doctorkafka.KafkaClusterManager;

import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/clusters/{clusterName}/brokers")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
public class BrokersApi extends DoctorKafkaApi {

    public BrokersApi(DoctorKafka drkafka){
        super(drkafka);
    }

    @GET
    public List<KafkaBroker> getBrokerList(@PathParam("clusterName") String clusterName) {
        KafkaClusterManager clusterManager = checkAndGetClusterManager(clusterName);
        return clusterManager.getAllBrokers();
    }

}
