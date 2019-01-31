package com.pinterest.doctorkafka.stats;

import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

// None of this information should ever change
public class HostStats {

  private static final Logger LOG = LogManager.getLogger(HostStats.class);

  private String availabilityZone = null;
  private String instanceType = null;
  private String amiId = null;
  private String hostname = null;

  public HostStats() {
    String value = null;
    BufferedReader input = null;
    try {
      Process process = Runtime.getRuntime().exec("ec2metadata");
      input = new BufferedReader(new InputStreamReader(process.getInputStream()));

      String outputLine;
      while ((outputLine = input.readLine()) != null) {
        String[] elements = outputLine.split(":");
        if (elements.length != 2) {
          continue;
        }
	value = elements[1].trim();
        if (elements[0].equals("availability-zone")) {
          availabilityZone = value;
        } else if (elements[0].equals("instance-type")) {
          instanceType = value;
        } else if (elements[0].equals("ami-id")) {
          amiId = value;
        } else if (elements[0].equals("hostname")) { // This only works if the hostname is explicitly set in AWS
          hostname = value;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to get ec2 metadata", e);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          LOG.error("Failed to close bufferReader", e);
        }
      }
    }
    // Not every Kafka cluster lives in AWS
    if (hostname == null) {
	hostname = OperatorUtil.getHostname();
    }
    LOG.info("set hostname to {}", hostname);
  }

  // getters
  public String getAvailabilityZone() {
    return availabilityZone;
  }

  public String getInstanceType() {
    return instanceType;
  }

  public String getAmiId() {
    return amiId;
  }

  public String getHostname() {
    return hostname;
  }

}
