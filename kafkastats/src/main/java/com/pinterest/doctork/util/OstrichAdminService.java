package com.pinterest.doctork.util;

import com.google.common.collect.Maps;
import com.twitter.ostrich.admin.AdminHttpService;
import com.twitter.ostrich.admin.AdminServiceFactory;
import com.twitter.ostrich.admin.CustomHttpHandler;
import com.twitter.ostrich.admin.RuntimeEnvironment;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Map$;
import scala.collection.immutable.List$;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OstrichAdminService implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(OstrichAdminService.class);
  private final int port;
  private final Map<String, CustomHttpHandler> customHttpHandlerMap = Maps.newHashMap();

  public OstrichAdminService(int port) {
    this.port = port;
  }

  public void addHandler(String path, CustomHttpHandler handler) {
    this.customHttpHandlerMap.put(path, handler);
  }

  public void startAdminHttpService() {
    try {
      Properties properties = new Properties();
      properties.load(this.getClass().getResource("build.properties").openStream());
      LOG.info("build.properties build_revision: {}",
          properties.getProperty("build_revision", "unknown"));
    } catch (Throwable t) {
      LOG.warn("Failed to load properties from build.properties", t);
    }
    Duration[] defaultLatchIntervals = {Duration.apply(1, TimeUnit.MINUTES)};
    Iterator<Duration> durationIterator = Arrays.asList(defaultLatchIntervals).iterator();
    @SuppressWarnings("deprecation")
    AdminServiceFactory adminServiceFactory = new AdminServiceFactory(
        this.port,
        20,
        List$.MODULE$.empty(),
        Option.empty(),
        List$.MODULE$.empty(),
        Map$.MODULE$.empty(),
        JavaConversions.asScalaIterator(durationIterator).toList());
    RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment(this);
    AdminHttpService service = adminServiceFactory.apply(runtimeEnvironment);
    for (Map.Entry<String, CustomHttpHandler> entry : this.customHttpHandlerMap.entrySet()) {
      service.httpServer().createContext(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void run() {
    startAdminHttpService();
  }
}
