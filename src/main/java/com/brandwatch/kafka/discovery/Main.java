package com.brandwatch.kafka.discovery;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Main {

    private static Options options;
    private static HelpFormatter helpFormatter;

    public static void main(String[] args) throws Exception {
        createOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption("host") || !commandLine.hasOption("port")) {
            printHelp();
            return;
        }

        KafkaBrokerDiscoverer kafkaBrokerDiscoverer = new KafkaBrokerDiscoverer(
                commandLine.getOptionValue("host"), commandLine.getOptionValue("port"));
        System.out.println(kafkaBrokerDiscoverer.getConnectionString());
        kafkaBrokerDiscoverer.close();
    }

    private static void printHelp() {
        helpFormatter.printHelp("kafka-broker-discovery [options]", options);
    }

    private static void createOptions() {
        options = new Options();
        options.addOption("h", "host", true, "Zookeeper host");
        options.addOption("p", "port", true, "Zookeeper port");
        helpFormatter = new HelpFormatter();
    }
}
