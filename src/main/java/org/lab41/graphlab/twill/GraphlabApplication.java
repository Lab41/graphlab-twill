package org.lab41.graphlab.twill;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class GraphlabApplication {

    private static Logger LOG = LoggerFactory.getLogger(GraphlabApplication.class);

    private static class GraphlabRunnable extends AbstractTwillRunnable {

        @Override
        public void run() {
            String zkStr = getArgument("zkStr");

            System.err.println("weee: " + zkStr);
            LOG.error("weee: " + zkStr);

            for(String x: getArguments().keySet()) {
                LOG.error("arg: " + x + " " + getArgument(x));
            }

            /*
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(zkStr)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();

            client.start();

            DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, "/mypath", 1);

            LOG.error("entering barrier");

            try {
                barrier.enter(5, TimeUnit.SECONDS);
            } catch (e) {
                LOG.error("hey", e);
                e.printStackTrace();
                return;
            }

            System.err.println("weee");
            LOG.error("in barrier");

            try {
                barrier.leave(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error("hey", e);
                e.printStackTrace();
                return;
            }
            */

            System.err.println("weee");
            LOG.error("out of barrier");
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Arguments format: <host:port of zookeeper server>");
            System.exit(1);
        }

        String zkStr = args[0];

        final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
        twillRunner.startAndWait();

        final TwillController controller = twillRunner.prepare(new GraphlabRunnable())
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                .withArguments("zkStr", zkStr)
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                controller.stopAndWait();
                twillRunner.stopAndWait();
            }
        });

        try {
            Services.getCompletionFuture(controller).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
