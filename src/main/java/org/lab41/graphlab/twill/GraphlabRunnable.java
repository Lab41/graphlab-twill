package org.lab41.graphlab.twill;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
* Created by etryzelaar on 5/20/14.
*/
class GraphlabRunnable extends AbstractTwillRunnable {

    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String SERVICE_NAME = "service";

    public GraphlabRunnable(String zkStr, String zkPath, int barrierSleep, int sleep) {
        super(ImmutableMap.of(
                "zkStr", zkStr,
                "zkPath", zkPath,
                "barrierWaitTime", Integer.toString(barrierSleep),
                "finishedSleepTime", Integer.toString(sleep)));
    }

    public void run() {
        String zkStr = getArgument("zkStr");
        String zkPath = getArgument("zkPath");
        int barrierWaitTime = Integer.parseInt(getArgument("barrierWaitTime"));
        int finishedSleepTime = Integer.parseInt(getArgument("finishedSleepTime"));

        TwillContext context = getContext();
        int instanceCount = context.getInstanceCount();
        int virtualCores = context.getVirtualCores();

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkStr)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace(zkPath)
                .build();

        client.start();

        try {
            DistributedDoubleBarrier barrier;
            barrier = new DistributedDoubleBarrier(client, "barrier", instanceCount);

            LOG.debug("entering barrier");

            if (instanceCount > 1) {
                if (!barrier.enter(finishedSleepTime, TimeUnit.SECONDS)) {
                    LOG.error("failed to enter barrier");
                    return;
                }
            }

            LOG.debug("entered barrier");

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

            LOG.debug("leaving barrier");

            if (instanceCount > 1) {
                if (!barrier.leave(barrierWaitTime, TimeUnit.SECONDS)) {
                    LOG.error("failed to leave barrier");
                    return;
                }
            }

            LOG.debug("left barrier");

            // FIXME: work around a Twill bug where Kafka won't send all the messages unless we sleep for a little bit.
            try {
                TimeUnit.SECONDS.sleep(finishedSleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            LOG.error("error", e);
            e.printStackTrace();
        }
    }
}
