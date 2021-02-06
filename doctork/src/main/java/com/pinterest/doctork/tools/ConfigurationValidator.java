package com.pinterest.doctork.tools;

import com.pinterest.doctork.plugins.manager.PluginManager;
import com.pinterest.doctork.plugins.task.TaskHandler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

import java.util.Map;
import java.util.Set;

/**
 * This class performs a dry run on a config file for validation purposes.
 * Usage:
 * java -cp <classpath> com.pinterest.doctork.tools.ConfigurationValidator -f <config file> [--run-configure]
 */
public class ConfigurationValidator {

  private static final Logger LOG = LogManager.getLogger(ConfigurationValidator.class);
  private static final Options options = new Options();
  private static final String ARG_CONFIG_FILE = "config-file";
  private static final String ARG_DRY_CONFIGURE = "run-configure";
  private static final String ARG_HELP = "help";

  private static CommandLine parseCommandLine(String[] args) {
    Option configFileOpt = new Option("f", ARG_CONFIG_FILE, true, "config file to validate");
    configFileOpt.setRequired(true);
    Option helpOpt = new Option("h", ARG_HELP, false, "shows this help message");
    Option dryConfigOpt = new Option(null, ARG_DRY_CONFIGURE, false, "enable dry run of the configure phase");

    options.addOption(configFileOpt);
    options.addOption(helpOpt);
    options.addOption(dryConfigOpt);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit(e);
    }
    return cmd;
  }

  private static void printUsageAndExit(Exception e) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ConfigurationValidator", e.toString(), options, null, true);
    System.exit(1);
  }

  public static void main(String[] args) {
    CommandLine commandLine = parseCommandLine(args);
    if (commandLine.hasOption(ARG_HELP)) {
      printUsageAndExit(null);
    }

    String configFile = commandLine.getOptionValue(ARG_CONFIG_FILE);
    com.pinterest.doctork.config.DoctorKConfig doctorkConfig = null;

    Set<String> zkurls = null;
    try {

      // config file path check
      if (!new File(configFile).exists()) {
        LOG.error("Config file doesn't exist!");
        System.exit(1);
      }

      // parse config, any syntax error should be raised here
      doctorkConfig = new com.pinterest.doctork.config.DoctorKConfig(configFile);
      zkurls = doctorkConfig.getClusterZkUrls();

      // check if clusters exist
      if (zkurls == null || zkurls.isEmpty()) {
        LOG.error("Cannot get zkurls of clusters!");
        System.exit(1);
      }

    } catch (Exception e) {
      LOG.error("Error when loading the config file.", e);
      System.exit(1);
    }

    Map<String, Configuration> baseMonitorConfigs = doctorkConfig.getMonitorsConfigs();
    Map<String, Configuration> baseOperatorConfigs = doctorkConfig.getOperatorsConfigs();
    Map<String, Configuration> baseActionConfigs = doctorkConfig.getActionsConfigs();

    for (String zkurl : zkurls) {
      PluginManager manager = new com.pinterest.doctork.plugins.manager.DoctorKPluginManager();
      com.pinterest.doctork.config.DoctorKClusterConfig clusterConfig = doctorkConfig.getClusterConfigByZkUrl(zkurl);
      String clusterName = clusterConfig.getClusterName();

      // load and configure actions
      Map<String, AbstractConfiguration>
          clusterMonitorConfigs =
          clusterConfig.getEnabledMonitorsConfigs(baseMonitorConfigs);
      Map<String, AbstractConfiguration>
          clusterOperatorConfigs =
          clusterConfig.getEnabledOperatorsConfigs(baseOperatorConfigs);
      Map<String, AbstractConfiguration>
          clusterActionConfigs =
          clusterConfig.getEnabledActionsConfigs(baseActionConfigs);

      LOG.info("Plugins on cluster {}", clusterName);
      LOG.info("  Monitor plugins:");
      for (String m : clusterMonitorConfigs.keySet()) {
        LOG.info("    {}", m);
      }
      LOG.info("  Cluster plugins:");
      for (String o : clusterOperatorConfigs.keySet()) {
        LOG.info("    {}", o);
      }
      LOG.info("  Action plugins:");
      for (String a : clusterActionConfigs.keySet()) {
        LOG.info("    {}", a);
      }

      if (commandLine.hasOption(ARG_DRY_CONFIGURE)) {
        String pluginName;
        for (Map.Entry<String, AbstractConfiguration> entry : clusterMonitorConfigs.entrySet()) {
          pluginName = entry.getKey();
          AbstractConfiguration config = entry.getValue();
          try {
            manager.getMonitor(config);
          } catch (Exception e) {
            LOG.error("Error when configuring monitor {} on cluster {}", pluginName, clusterName, e);
            System.exit(1);
          }
        }
        for (Map.Entry<String, AbstractConfiguration> entry : clusterOperatorConfigs.entrySet()) {
          pluginName = entry.getKey();
          AbstractConfiguration config = entry.getValue();
          try {
            manager.getOperator(config);
          } catch (Exception e) {
            LOG.error("Error when configuring operator {} on cluster {}", pluginName, clusterName, e);
            System.exit(1);
          }
        }
        for (Map.Entry<String, AbstractConfiguration> entry : clusterActionConfigs.entrySet()) {
          pluginName = entry.getKey();
          AbstractConfiguration config = entry.getValue();
          try {
            TaskHandler handler = manager.getAction(config);
            LOG.info("TaskHandler {} on cluster {} subscribes to these tasks: {}", pluginName, clusterName, handler.getSubscribedTasks());
          } catch (Exception e) {
            LOG.error("Error when configuring action {} on cluster {}", pluginName, clusterName, e);
          }
        }
      }
    }
  }
}
