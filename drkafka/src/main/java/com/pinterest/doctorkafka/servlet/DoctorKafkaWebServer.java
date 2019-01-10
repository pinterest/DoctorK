package com.pinterest.doctorkafka.servlet;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DoctorKafkaWebServer implements Runnable {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaWebServer.class);
  private static final String WEBAPP_DIR = "webapp/pages/index.html";

  private Thread thread;
  private int port;

  public DoctorKafkaWebServer(int port) {
    this.port = port;
  }

  public void start() {
    thread = new Thread(this);
    thread.start();
  }

  @Override
  public void run() {

    try {
      Server server = new Server(port);
      URL warUrl = DoctorKafkaWebServer.class.getClassLoader().getResource(WEBAPP_DIR);
      if (warUrl == null) {
        LOG.error("warUrl is null");
      }

      String warUrlString = warUrl.toExternalForm();
      WebAppContext webapp = new WebAppContext();

      server.setHandler(webapp);
      server.start();
      if (LOG.isDebugEnabled()) {
        server.dumpStdErr();
      }
      server.join();
    } catch (Exception e) {
      LOG.error("Exception in DoctorKafkaWebServer.", e);
    }
  }

  @SuppressWarnings("serial")
  public static class OperatorIndexServlet extends HttpServlet {
    private static final String indexPagePath = "webapp/pages/index.html";
    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws ServletException,
                                                              IOException {
      response.setContentType("text/html");
      try {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource(indexPagePath);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{resource});

        InputStream input = urlClassLoader.getResourceAsStream(indexPagePath);
        String result = CharStreams.toString(new InputStreamReader(input, Charsets.UTF_8));
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().print(result);
      } catch (Exception e) {
        response.getWriter().print(e);
        LOG.error("error : ", e);
      }
    }
  }
}
