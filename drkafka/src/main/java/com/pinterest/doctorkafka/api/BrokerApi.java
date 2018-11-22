package com.pinterest.doctorkafka.api;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.pinterest.doctorkafka.DoctorKafkaMain;
import com.pinterest.doctorkafka.KafkaClusterManager;

@Path("/cluster/{clusterName}/broker")
public class BrokerApi {

    @GET
    public List<String> getBrokerList(@PathParam("clusterName") String clusterName) {
        KafkaClusterManager clusterManager = DoctorKafkaMain.doctorKafka.getClusterManager(clusterName);
        return clusterManager.getAllBrokers().stream().map(b -> b.name()).collect(Collectors.toList());
    }

}
