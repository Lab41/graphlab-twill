package org.lab41.graphlab.twill;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.synchronization.DoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
* Created by etryzelaar on 5/20/14.
*/
class GraphlabRunnable extends AbstractTwillRunnable {

    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String SERVICE_NAME = "graphlab";

    public GraphlabRunnable(String zkStr, String barrierName, int barrierWaitTime, int finishedSleepTime) {
        super(ImmutableMap.of(
                "zkStr", zkStr,
                "barrierName", barrierName,
                "barrierWaitTime", Integer.toString(barrierWaitTime),
                "finishedSleepTime", Integer.toString(finishedSleepTime),
                "jobName", UUID.randomUUID().toString()));
    }

    public void run() {
        String zkStr = getArgument("zkStr");
        String barrierName = getArgument("barrierName");
        int barrierWaitTime = Integer.parseInt(getArgument("barrierWaitTime"));
        int finishedSleepTime = Integer.parseInt(getArgument("finishedSleepTime"));
        String jobName = getArgument("jobName");

        TwillContext context = getContext();
        int instanceCount = context.getInstanceCount();
        int virtualCores = context.getVirtualCores();

        Cancellable cancellable = this.getContext().announce(SERVICE_NAME, 0);

        try {
            LOG.debug("creating barrier {} with {} parties", barrierName, instanceCount);

            DoubleBarrier barrier = this.getContext().getDoubleBarrier(barrierName, instanceCount);

            LOG.debug("entering barrier");

            barrier.enter(barrierWaitTime, TimeUnit.SECONDS);

            LOG.debug("entered barrier");

            List<String> args = ImmutableList.of(
                    "/home/etryzelaar/run"
            );

            ProcessBuilder processBuilder = new ProcessBuilder(args)
                    .redirectErrorStream(true);

            Map<String, String> env = processBuilder.environment();
            env.put("ZK_SERVERS", zkStr);
            env.put("ZK_JOBNAME", jobName);
            env.put("ZK_NUMNODES", Integer.toString(instanceCount));

            Process process = processBuilder.start();

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

            barrier.leave(barrierWaitTime, TimeUnit.SECONDS);

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

        cancellable.cancel();
    }
}
