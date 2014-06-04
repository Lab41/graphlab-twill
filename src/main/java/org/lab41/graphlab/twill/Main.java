package org.lab41.graphlab.twill;

import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Main {

    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        CommandLineParser parser = new BasicParser();

        Options options = new Options();
        options.addOption("i", "instances", true, "number of instances");
        options.addOption("t", "threads", true, "number of threads");
        options.addOption("b", "barrier-wait-time", true, "how long to sleep for");
        options.addOption("f", "finished-sleep-time", true, "how long to sleep for");

        int instanceCount = 1;
        int virtualCores = 1;
        int barrierWaitTime = 10;
        int finishedSleepTime = 10;

        try {
            CommandLine line = parser.parse(options, args);

            String option;
            if ((option = options.getOption("instances").getValue()) != null) {
                instanceCount = Integer.parseInt(option);
            }

            if ((option = options.getOption("threads").getValue()) != null) {
                virtualCores = Integer.parseInt(option);
            }

            if ((option = options.getOption("barrier-wait-time").getValue()) != null) {
                barrierWaitTime = Integer.parseInt(option);
            }

            if ((option = options.getOption("finished-sleep-time").getValue()) != null) {
                finishedSleepTime = Integer.parseInt(option);
            }

            args = line.getArgs();

        } catch (ParseException e) {
            System.exit(1);
        }

        if (args.length != 1) {
            System.err.println("Arguments format: <host:port of zookeeper server>");
            System.exit(1);
        }

        String zkStr = args[0];

        final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
        twillRunner.startAndWait();

        ResourceSpecification resources = ResourceSpecification.Builder.with()
                .setVirtualCores(virtualCores)
                .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(instanceCount)
                .build();

        String zkPath = UUID.randomUUID().toString();

        final TwillController controller = twillRunner.prepare(new GraphlabRunnable("barrier", barrierWaitTime, finishedSleepTime), resources)
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                //.enableDebugging()
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
