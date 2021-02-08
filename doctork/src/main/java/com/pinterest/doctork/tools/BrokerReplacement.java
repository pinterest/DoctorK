package com.pinterest.doctork.tools;

import com.pinterest.doctork.util.BrokerReplacer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class BrokerReplacement {

  private static final String BROKER = "broker";
  private static final String COMMAND = "command";
  private static final Options options = new Options();

  /**
   *  Usage: BrokerReplacement -broker broker1  -command "relaunch host script"
   */
  private static CommandLine parseCommandLine(String[] args) {
    Option broker = new Option(BROKER, true, "broker name");
    Option command = new Option(COMMAND, true, "command for relaunching a host");
    options.addOption(broker).addOption(command);

    if (args.length < 3) {
      printUsageAndExit();
    }

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit();
    }
    return cmd;
  }

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("BrokerReplacement", options);
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    CommandLine commandLine = parseCommandLine(args);
    String broker = commandLine.getOptionValue(BROKER);
    String command = commandLine.getOptionValue(COMMAND);

    BrokerReplacer brokerReplacer = new BrokerReplacer(command);
    brokerReplacer.replaceBroker(broker);
    System.out.print("Broker replacement for " + broker + " in progress ");
    while (brokerReplacer.busy()) {
      System.out.print(".");
      Thread.sleep(10000);
    }
    System.out.println("Finished broker replacement for " + broker);
  }
}
