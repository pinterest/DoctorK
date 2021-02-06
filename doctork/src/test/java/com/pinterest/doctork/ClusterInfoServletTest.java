package com.pinterest.doctork;

import com.pinterest.doctork.servlet.ClusterInfoServlet;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Console;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ClusterInfoServletTest extends Mockito {

  @Test
  public void clusterInfoJSONResponse() throws Exception {
    String clusterName = "cluster1";
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    DoctorK mockDoctor = mock(DoctorK.class);
    DoctorKMain.doctorK = mockDoctor;
    KafkaClusterManager clusterManager = mock(KafkaClusterManager.class);
    KafkaCluster cluster = new KafkaCluster(clusterName, 1440);

    when(mockDoctor.getClusterManager(clusterName)).thenReturn(clusterManager);
    when(clusterManager.getCluster()).thenReturn(cluster);

    // make sure we can see the response
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(writer);

    // pretend our client asked for json
    when(request.getQueryString()).thenReturn("name=" + clusterName);
    when(request.getHeader("content-type")).thenReturn("application/json");
    new ClusterInfoServlet().doGet(request, response);

    String output = stringWriter.getBuffer().toString();
    Console.println(output);
  }

}
