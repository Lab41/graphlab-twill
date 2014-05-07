package org.lab41.graphlab.twill;

import com.google.common.collect.ImmutableMap;
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

import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class GraphlabApplication {

    private static Logger LOG = LoggerFactory.getLogger(GraphlabApplication.class);

    private static class GraphlabRunnable extends AbstractTwillRunnable {

        public GraphlabRunnable(String zkStr, int count) {
            super(ImmutableMap.of("zkStr", zkStr, "count", Integer.toString(count)));
        }

        @Override
        public void run() {
            String zkStr = getArgument("zkStr");
            int count = Integer.parseInt(getArgument("count"));

            System.err.println("weee: " + zkStr);
            LOG.error("weee: " + zkStr);

            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(zkStr)
                    .retryPolicy(new ExponentialBackoffRetry(1000, count))
                    .build();

            client.start();

            DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, "/mypath", count);

            LOG.error("entering barrier");

            try {
                if (!barrier.enter(1, TimeUnit.SECONDS)) {
                    LOG.error("failed to enter barrier");
                    return;
                }

                LOG.error("in barrier");

                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                LOG.error("woken up");

                if (!barrier.leave(1, TimeUnit.SECONDS)) {
                    LOG.error("failed to leave barrier");
                    return;
                }

                LOG.error("out of barrier");

            } catch (Exception e) {
                LOG.error("error", e);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Arguments format: <host:port of zookeeper server>");
            System.exit(1);
        }

        String zkStr = args[0];
        int count = Integer.parseInt(args[1]);
        int instances = Integer.parseInt(args[2]);

        final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
        twillRunner.startAndWait();

        ResourceSpecification resources = ResourceSpecification.Builder
                .with()
                .setVirtualCores(1)
                .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(instances)
                .build();

        final TwillController controller = twillRunner.prepare(new GraphlabRunnable(zkStr, count), resources)
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
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
