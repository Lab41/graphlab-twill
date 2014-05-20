package org.lab41.graphlab.twill;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {

    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static class GraphlabRunnable extends AbstractTwillRunnable {

        public GraphlabRunnable(String zkStr, int instanceCount, int threadCount, int barrierSleep, int sleep) {
            super(ImmutableMap.of(
                    "zkStr", zkStr,
                    "instanceCount", Integer.toString(instanceCount),
                    "threadCount", Integer.toString(threadCount),
                    "barrierWaitTime", Integer.toString(barrierSleep),
                    "finishedSleepTime", Integer.toString(sleep)));
        }

        public void run() {
            String zkStr = getArgument("zkStr");
            int instanceCount = Integer.parseInt(getArgument("instanceCount"));
            int threadCount = Integer.parseInt(getArgument("threadCount"));
            int barrierWaitTime = Integer.parseInt(getArgument("barrierWaitTime"));
            int finishedSleepTime = Integer.parseInt(getArgument("finishedSleepTime"));

            CuratorFramework client;

            if (instanceCount > 1) {
                client = CuratorFrameworkFactory.builder()
                        .connectString(zkStr)
                        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                        .namespace("GraphlabApp")
                        .build();

                client.start();
            }

            String name = "barrier"; //UUID.randomUUID().toString()

            try {
                DistributedDoubleBarrier barrier;

                /*
                if (instanceCount > 1) {
                    barrier = new DistributedDoubleBarrier(client, name, instanceCount);
                }
                */

                LOG.error("entering barrier");

                /*
                if (instanceCount > 1) {
                    if (!barrier.enter(finishedSleepTime, TimeUnit.SECONDS)) {
                        LOG.error("failed to enter barrier");
                        return;
                    }
                }
                */

                LOG.error("in barrier");

                List<String> args = ImmutableList.of(
                        "/home/etryzelaar/run"
                );

                Process process = new ProcessBuilder(args)
                        .redirectErrorStream(true)
                        .start();

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charsets.US_ASCII))) {
                    String line = reader.readLine();
                    while (line != null) {
                        LOG.info(line);
                        line = reader.readLine();
                    }
                }

                // Ignore errors for now.
                process.waitFor();

                LOG.error("woken up");

                /*
                if (instanceCount > 1) {
                    if (!barrier.leave(barrierWaitTime, TimeUnit.SECONDS)) {
                        LOG.error("failed to leave barrier");
                        return;
                    }
                }
                */

                LOG.error("out of barrier");

                // FIXME: work around a Twill bug where Kafka won't send all the messages unless we sleep for a little bit.
                try {
                    TimeUnit.SECONDS.sleep(finishedSleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                LOG.error("done");

            } catch (Exception e) {
                LOG.error("error", e);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        CommandLineParser parser = new BasicParser();

        Options options = new Options();
        options.addOption("i", "instances", true, "number of instances");
        options.addOption("t", "threads", true, "number of threads");
        options.addOption("b", "barrier-wait-time", true, "how long to sleep for");
        options.addOption("f", "finished-sleep-time", true, "how long to sleep for");

        int instanceCount = 1;
        int threadCount = 1;
        int barrierWaitTime = 10;
        int finishedSleepTime = 10;

        try {
            CommandLine line = parser.parse(options, args);

            String option;
            if ((option = options.getOption("instances").getValue()) != null) {
                instanceCount = Integer.parseInt(option);
            }

            if ((option = options.getOption("threads").getValue()) != null) {
                threadCount = Integer.parseInt(option);
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

        ResourceSpecification resources = ResourceSpecification.Builder
                .with()
                .setVirtualCores(threadCount)
                .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(instanceCount)
                .build();

        final TwillController controller = twillRunner.prepare(new GraphlabRunnable(zkStr, instanceCount, threadCount, barrierWaitTime, finishedSleepTime), resources)
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
