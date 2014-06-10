/**
 * Copyright 2014 In-Q-Tel/Lab41
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lab41.graphlab.twill;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.twill.api.*;
import org.apache.twill.synchronization.DoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
* Created by etryzelaar on 5/20/14.
*/
class GraphlabRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final int BARRIER_WAIT_TIME = 60;
    private static final TimeUnit BARRIER_WAIT_UNIT = TimeUnit.SECONDS;

    private Arguments arguments;

    @Override
    public TwillRunnableSpecification configure() {
        return TwillRunnableSpecification.Builder.with()
                .setName(getClass().getSimpleName())
                .noConfigs()
                .build();
    }

    @Override
    public void initialize(TwillContext context) {
        super.initialize(context);
        arguments = Arguments.fromArray(context.getArguments());
    }

    @Override
    public void handleCommand(Command command) throws Exception {

    }

    @Override
    public void stop() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public void run() {
        Preconditions.checkNotNull(arguments);

        TwillContext context = getContext();
        Preconditions.checkNotNull(context);

        String graphlabPath = arguments.getGraphlabPath();
        Preconditions.checkNotNull(graphlabPath);

        String[] graphlabArgs = arguments.getGraphlabArgs();
        Preconditions.checkNotNull(graphlabArgs);

        List<String> args = new ArrayList<>();
        args.add(graphlabPath);
        Collections.addAll(args, graphlabArgs);

        String zkStr = System.getenv("TWILL_ZK_CONNECT");
        Preconditions.checkNotNull(zkStr);

        int instanceCount = context.getInstanceCount();
        String runId = context.getApplicationRunId().getId();
        String barrierName = runId + "/barrier";

        try {
            LOG.debug("creating barrier {} with {} parties", barrierName, instanceCount);

            DoubleBarrier barrier = this.getContext().getDoubleBarrier(barrierName, instanceCount);

            LOG.debug("entering barrier");

            barrier.enter(BARRIER_WAIT_TIME, BARRIER_WAIT_UNIT);

            try {
                LOG.debug("entered barrier");

                ProcessBuilder processBuilder = new ProcessBuilder(args)
                        .redirectErrorStream(true);

                Map<String, String> env = processBuilder.environment();
                env.put("ZK_SERVERS", zkStr);
                env.put("ZK_JOBNAME", "graphlab-workers");
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
            } finally {
                LOG.debug("leaving barrier");

                barrier.leave(BARRIER_WAIT_TIME, BARRIER_WAIT_UNIT);

                LOG.debug("left barrier");
            }

            // FIXME: work around a Twill bug where Kafka won't send all the messages unless we sleep for a little bit.
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            LOG.error("error", e);
            e.printStackTrace();
        }
    }

    public static class Arguments {
        private final String graphlabPath;
        private final String[] graphlabArgs;

        public Arguments(String graphlabPath, String[] graphlabArgs) {
            this.graphlabPath = graphlabPath;
            this.graphlabArgs = graphlabArgs;
        }

        public String getGraphlabPath() {
            return graphlabPath;
        }

        public String[] getGraphlabArgs() {
            return graphlabArgs;
        }

        public String[] toArray() {
            String[] result = new String[1 + graphlabArgs.length];
            result[0] = graphlabPath;
            System.arraycopy(graphlabArgs, 0, result, 1, graphlabArgs.length);
            return result;
        }

        public static Arguments fromArray(String[] args) {
            Builder builder = new Builder();
            builder.setGraphlabPath(args[0]);
            builder.setGraphlabArgs(Arrays.copyOfRange(args, 1, args.length));
            return builder.createArguments();
        }

        public static class Builder {
            private String graphlabPath;
            private String[] graphlabArgs;

            public Builder() {}

            public Builder setGraphlabPath(String graphlabPath) {
                this.graphlabPath = graphlabPath;
                return this;
            }

            public Builder setGraphlabArgs(String... graphlabArgs) {
                this.graphlabArgs = graphlabArgs;
                return this;
            }

            public Arguments createArguments() {
                return new Arguments(graphlabPath, graphlabArgs);
            }
        }
    }
}
