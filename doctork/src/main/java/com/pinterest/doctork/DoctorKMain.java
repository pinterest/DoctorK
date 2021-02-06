package com.pinterest.doctork;

import com.pinterest.doctork.api.BrokersApi;
import com.pinterest.doctork.api.BrokersDecommissionApi;
import com.pinterest.doctork.api.ClustersApi;
import com.pinterest.doctork.api.ClustersMaintenanceApi;
import com.pinterest.doctork.api.TopicsApi;
import com.pinterest.doctork.config.DoctorKAppConfig;
import com.pinterest.doctork.config.DoctorKAppConfig;
import com.pinterest.doctork.config.DoctorKConfig;
import com.pinterest.doctork.security.DoctorKAuthorizationFilter;
import com.pinterest.doctork.servlet.ClusterInfoServlet;
import com.pinterest.doctork.servlet.DoctorKActionsServlet;
import com.pinterest.doctork.servlet.DoctorKBrokerStatsServlet;
import com.pinterest.doctork.servlet.DoctorKInfoServlet;
import com.pinterest.doctork.servlet.KafkaTopicStatsServlet;
import com.pinterest.doctork.servlet.UnderReplicatedPartitionsServlet;
import com.pinterest.doctork.util.OperatorUtil;

import com.google.common.collect.ImmutableList;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.jetty.GzipHandlerFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.request.logging.LogbackAccessRequestLogFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

/**
 * DoctorK is the central service for managing kafka operation.
 *
 */
public class DoctorKMain extends Application<DoctorKAppConfig> {

  private static final Logger LOG = LogManager.getLogger(com.pinterest.doctork.DoctorKMain.class);
  private static final String OSTRICH_PORT = "ostrichport";

  public static DoctorK doctorK = null;
  private static DoctorKWatcher operatorWatcher = null;

  @Override
  public void initialize(Bootstrap<DoctorKAppConfig> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/webapp/pages/", "/", "index.html"));
    bootstrap.addBundle(new AssetsBundle("/webapp2/build/", "/", "index.html"));
  }

  @Override
  public void run(DoctorKAppConfig configuration, Environment environment) throws Exception {
    Runtime.getRuntime().addShutdownHook(new com.pinterest.doctork.DoctorKMain.OperatorCleanupThread());

    LOG.info("Configuration path : {}", configuration.getConfig());

    DoctorKConfig doctorkConfig = new DoctorKConfig(configuration.getConfig());

    if (!doctorkConfig.getRestartDisabled()){
      operatorWatcher = new DoctorKWatcher(doctorkConfig.getRestartIntervalInSeconds());
      operatorWatcher.start();
    }

    configureServerRuntime(configuration, doctorkConfig);

    doctorK = new DoctorK(doctorkConfig);

    registerAPIs(environment, doctorK, doctorkConfig);
    registerServlets(environment);

    Executors.newCachedThreadPool().submit(() -> {
      try {
        doctorK.start();
        LOG.info("DoctorK started");
      } catch (Exception e) {
        LOG.error("DoctorK start failed", e);
      }
    });

    startMetricsService(doctorkConfig);
    LOG.info("DoctorK API server started");
  }

  private void configureServerRuntime(DoctorKAppConfig configuration, DoctorKConfig config) {
    DefaultServerFactory defaultServerFactory = 
        (DefaultServerFactory) configuration.getServerFactory();

    // Disable gzip compression for HTTP, this is required in-order to make
    // Server-Sent-Events work, else due to GZIP the browser waits for entire chunks
    // to arrive thereby the UI receiving no events
    // We are programmatically disabling it here so it makes it easy to launch
    // Firefly
    GzipHandlerFactory gzipHandlerFactory = new GzipHandlerFactory();
    gzipHandlerFactory.setEnabled(false);
    defaultServerFactory.setGzipFilterFactory(gzipHandlerFactory);
    // Note that if someone explicitly enables gzip in the Dropwizard config YAML
    // then
    // this setting will be over-ruled causing the UI to stop working

    // Disable HTTP request logging
    LogbackAccessRequestLogFactory accessRequestLogFactory = new LogbackAccessRequestLogFactory();
    accessRequestLogFactory.setAppenders(ImmutableList.of());
    defaultServerFactory.setRequestLogFactory(accessRequestLogFactory);

    // Disable admin connector
    defaultServerFactory.setAdminConnectors(ImmutableList.of());

    // Configure bind host and port number
    HttpConnectorFactory application = (HttpConnectorFactory) HttpConnectorFactory.application();
    application.setPort(config.getWebserverPort());
    application.setBindHost(config.getWebserverBindHost());
    defaultServerFactory.setApplicationConnectors(Collections.singletonList(application));
  }

  private void registerAPIs(Environment environment, DoctorK doctorK, DoctorKConfig doctorKConfig) {
    environment.jersey().setUrlPattern("/api/*");
    checkAndInitializeAuthorizationFilter(environment, doctorKConfig);
    environment.jersey().register(new BrokersApi(doctorK));
    environment.jersey().register(new ClustersApi(doctorK));
    environment.jersey().register(new TopicsApi(doctorK));
    environment.jersey().register(new ClustersMaintenanceApi(doctorK));
    environment.jersey().register(new BrokersDecommissionApi(doctorK));
  }

  private void checkAndInitializeAuthorizationFilter(Environment environment, DoctorKConfig doctorKConfig) {
    LOG.info("Checking authorization filter");
    try {
      Class<? extends DoctorKAuthorizationFilter> authorizationFilterClass = doctorKConfig.getAuthorizationFilterClass();
      if (authorizationFilterClass != null) {
        DoctorKAuthorizationFilter filter = authorizationFilterClass.newInstance();
        filter.configure(doctorKConfig);
        LOG.info("Using authorization filer:" + filter.getClass().getName());
        environment.jersey().register(filter);
        environment.jersey().register(RolesAllowedDynamicFeature.class);
      }
    } catch (Exception e) {
      LOG.error("Failed to get and initialize DoctorKAuthorizationFilter", e);
    }
  }

  private void startMetricsService(DoctorKConfig doctorkConfig) {
    int ostrichPort = doctorkConfig.getOstrichPort();
    String tsdHost = doctorkConfig.getTsdHost();
    int tsdPort = doctorkConfig.getTsdPort();
    if (tsdHost == null && tsdPort == 0 && ostrichPort == 0) {
      LOG.info("OpenTSDB and Ostrich options missing, not starting Ostrich service");
    } else if (ostrichPort == 0) {
      throw new NoSuchElementException(
          String.format("Key '%s' does not map to an existing object!", OSTRICH_PORT));
    } else {
      OperatorUtil.startOstrichService("doctork", tsdHost, tsdPort, ostrichPort);
    }
  }

  private void registerServlets(Environment environment) {
    environment.getApplicationContext().addServlet(ClusterInfoServlet.class,
        "/servlet/clusterinfo");
    environment.getApplicationContext().addServlet(KafkaTopicStatsServlet.class,
        "/servlet/topicstats");
    environment.getApplicationContext().addServlet(DoctorKActionsServlet.class,
        "/servlet/actions");
    environment.getApplicationContext().addServlet(DoctorKInfoServlet.class, "/servlet/info");
    environment.getApplicationContext().addServlet(DoctorKBrokerStatsServlet.class,
        "/servlet/brokerstats");
    environment.getApplicationContext().addServlet(UnderReplicatedPartitionsServlet.class,
        "/servlet/urp");
  }

  public static void main(String[] args) throws Exception {
    new com.pinterest.doctork.DoctorKMain().run(args);
  }

  static class OperatorCleanupThread extends Thread {

    @Override
    public void run() {
      try {
        if (doctorK != null) {
          doctorK.stop();
        }
      } catch (Throwable t) {
        LOG.error("Failure in stopping operator", t);
      }

      try {
        if (operatorWatcher != null) {
          operatorWatcher.stop();
        }
      } catch (Throwable t) {
        LOG.error("Shutdown failure in collectorMonitor : ", t);
      }
    }
  }

}
