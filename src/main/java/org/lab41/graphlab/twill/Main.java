package org.lab41.graphlab.twill;

import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.*;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        CommandLineParser parser = new GnuParser();

        Options options = new Options();

        options.addOption("h", "help", false, "print this message");
        options.addOption("i", "instances", true, "number of instances");
        options.addOption("t", "threads", true, "number of threads");

        int instanceCount = 1;
        int virtualCores = 1;

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("java -cp twill-graphlab-1.0-SNAPSHOT.jar org.lab41.graphlab.twill.Main [options] zookeeper-address graphlab-path", options);
                System.exit(0);
            }

            if (line.hasOption("instances")) {
                String option = line.getOptionValue("instances");
                instanceCount = Integer.parseInt(option);
            }

            if (line.hasOption("threads")) {
                String option = line.getOptionValue("threads");
                virtualCores = Integer.parseInt(option);
            }

            args = line.getArgs();

        } catch (ParseException e) {
            System.out.println("error: " + e);
            System.exit(1);
        }

        if (args.length != 2) {
            System.err.println("Arguments format: <host:port of zookeeper server> graphlab-path [graphlab-args]");
            System.exit(1);
        }

        String zkStr = args[0];
        GraphlabRunnable.Arguments arguments = GraphlabRunnable.Arguments.fromArray(
                Arrays.copyOfRange(args, 1, args.length));

        final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
        twillRunner.startAndWait();

        ResourceSpecification resources = ResourceSpecification.Builder.with()
                .setVirtualCores(virtualCores)
                .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(instanceCount)
                .build();

        final TwillController controller = twillRunner.prepare(new GraphlabRunnable(), resources)
                .withArguments("GraphlabRunnable", arguments.toArray())
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                //.enableDebugging(true)
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.error("shutting down");
                controller.stopAndWait();
                twillRunner.stopAndWait();
            }
        });

        LOG.error("before getting completion");

        try {
            Services.getCompletionFuture(controller).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        LOG.error("after shutting down");
    }

}
