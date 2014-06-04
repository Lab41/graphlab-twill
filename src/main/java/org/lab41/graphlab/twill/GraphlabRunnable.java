package org.lab41.graphlab.twill;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.synchronization.DoubleBarrier;
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

    public GraphlabRunnable(String barrierName, int barrierWaitTime, int finishedSleepTime) {
        super(ImmutableMap.of(
                "barrierName", barrierName,
                "barrierWaitTime", Integer.toString(barrierWaitTime),
                "finishedSleepTime", Integer.toString(finishedSleepTime)));
    }

    public void run() {
        String barrierName = getArgument("barrierName");
        int barrierWaitTime = Integer.parseInt(getArgument("barrierWaitTime"));
        int finishedSleepTime = Integer.parseInt(getArgument("finishedSleepTime"));

        TwillContext context = getContext();
        int instanceCount = context.getInstanceCount();
        int virtualCores = context.getVirtualCores();

        try {
            LOG.debug("creating barrier {} with {} parties", barrierName, instanceCount);

            DoubleBarrier barrier = this.getContext().getDoubleBarrier(barrierName, instanceCount);

            LOG.debug("entering barrier");

            barrier.enter(finishedSleepTime, TimeUnit.SECONDS);

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
    }
}
