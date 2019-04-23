package com.pinterest.doctorkafka;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.pinterest.doctorkafka.api.MaintenanceApi;
import com.pinterest.doctorkafka.api.BrokerApi;
import com.pinterest.doctorkafka.api.ClusterApi;
import com.pinterest.doctorkafka.config.DoctorKafkaAppConfig;
import com.pinterest.doctorkafka.config.DoctorKafkaConfig;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;
import com.pinterest.doctorkafka.servlet.ClusterInfoServlet;
import com.pinterest.doctorkafka.servlet.DoctorKafkaActionsServlet;
import com.pinterest.doctorkafka.servlet.DoctorKafkaBrokerStatsServlet;
import com.pinterest.doctorkafka.servlet.DoctorKafkaInfoServlet;
import com.pinterest.doctorkafka.servlet.KafkaTopicStatsServlet;
import com.pinterest.doctorkafka.servlet.UnderReplicatedPartitionsServlet;
import com.pinterest.doctorkafka.util.OperatorUtil;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.jetty.GzipHandlerFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.request.logging.LogbackAccessRequestLogFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * DoctorKafka is the central service for managing kafka operation.
 *
 */
public class DoctorKafkaMain extends Application<DoctorKafkaAppConfig> {

  private static final Logger LOG = LogManager.getLogger(DoctorKafkaMain.class);
  private static final String OSTRICH_PORT = "ostrichport";

  public static DoctorKafka doctorKafka = null;
  private static DoctorKafkaWatcher operatorWatcher = null;
  public static ReplicaStatsManager replicaStatsManager = null;

  @Override
  public void initialize(Bootstrap<DoctorKafkaAppConfig> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/webapp/pages/", "/", "index.html"));
  }

  @Override
  public void run(DoctorKafkaAppConfig configuration, Environment environment) throws Exception {
    Runtime.getRuntime().addShutdownHook(new DoctorKafkaMain.OperatorCleanupThread());

    LOG.info("Configuration path : {}", configuration.getConfig());

    replicaStatsManager = new ReplicaStatsManager(new DoctorKafkaConfig(configuration.getConfig()));

    if (!replicaStatsManager.getConfig().getRestartDisabled()){
      operatorWatcher = new DoctorKafkaWatcher(replicaStatsManager.getConfig().getRestartIntervalInSeconds());
      operatorWatcher.start();
    }

    configureServerRuntime(configuration, replicaStatsManager.getConfig());

    doctorKafka = new DoctorKafka(replicaStatsManager);

    registerAPIs(environment, doctorKafka);
    registerServlets(environment);

    Executors.newCachedThreadPool().submit(() -> {
      try {
        doctorKafka.start();
        LOG.info("DoctorKafka started");
      } catch (Exception e) {
        LOG.error("DoctorKafka start failed", e);
      }
    });

    startMetricsService();
    LOG.info("DoctorKafka API server started");
  }

  private void configureServerRuntime(DoctorKafkaAppConfig configuration, DoctorKafkaConfig config) {
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
    defaultServerFactory.setApplicationConnectors(Collections.singletonList(application));
  }

  private void registerAPIs(Environment environment, DoctorKafka doctorKafka) {
    environment.jersey().setUrlPattern("/api/*");
    environment.jersey().register(new BrokerApi());
    environment.jersey().register(new ClusterApi(doctorKafka));
    environment.jersey().register(new MaintenanceApi(doctorKafka));
  }

  private void startMetricsService() {
    int ostrichPort = replicaStatsManager.getConfig().getOstrichPort();
    String tsdHostPort = replicaStatsManager.getConfig().getTsdHostPort();
    if (tsdHostPort == null && ostrichPort == 0) {
      LOG.info("OpenTSDB and Ostrich options missing, not starting Ostrich service");
    } else if (ostrichPort == 0) {
      throw new NoSuchElementException(
          String.format("Key '%s' does not map to an existing object!", OSTRICH_PORT));
    } else {
      OperatorUtil.startOstrichService("doctorkafka", tsdHostPort, ostrichPort);
    }
  }

  private void registerServlets(Environment environment) {
    environment.getApplicationContext().addServlet(ClusterInfoServlet.class,
        "/servlet/clusterinfo");
    environment.getApplicationContext().addServlet(KafkaTopicStatsServlet.class,
        "/servlet/topicstats");
    environment.getApplicationContext().addServlet(DoctorKafkaActionsServlet.class,
        "/servlet/actions");
    environment.getApplicationContext().addServlet(DoctorKafkaInfoServlet.class, "/servlet/info");
    environment.getApplicationContext().addServlet(DoctorKafkaBrokerStatsServlet.class,
        "/servlet/brokerstats");
    environment.getApplicationContext().addServlet(UnderReplicatedPartitionsServlet.class,
        "/servlet/urp");
  }

  public static void main(String[] args) throws Exception {
    new DoctorKafkaMain().run(args);
  }

  static class OperatorCleanupThread extends Thread {

    @Override
    public void run() {
      try {
        if (doctorKafka != null) {
          doctorKafka.stop();
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
