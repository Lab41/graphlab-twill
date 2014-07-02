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
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        CommandLineParser parser = new GnuParser();

        Options options = new Options();

        options.addOption("h", "help", false, "print this message");
        options.addOption("i", "instances", true, "number of instances");
        options.addOption("t", "threads", true, "number of threads");
        options.addOption("debug", false, "enable debugging");

        int instanceCount = 1;
        int virtualCores = 1;
        boolean debug = false;

        try {
            CommandLine line = parser.parse(options, args, true);

            if (line.hasOption("help")) {
                printHelp(options);
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

            if (line.hasOption("debug")) {
                debug = true;
            }

            args = line.getArgs();

        } catch (ParseException e) {
            System.out.println("error: " + e);
            System.exit(1);
        }

        if (args.length != 5) {
            printHelp(options);
            System.exit(1);
        }

        String zkStr = args[0];
        GraphLabRunnable.Arguments arguments = GraphLabRunnable.Arguments.fromArray(
                Arrays.copyOfRange(args, 1, args.length));

        final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
        twillRunner.startAndWait();

        ResourceSpecification resources = ResourceSpecification.Builder.with()
                .setVirtualCores(virtualCores)
                .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                .setInstances(instanceCount)
                .build();

        String runnableName = "GraphLabRunnable";

        TwillPreparer preparer = twillRunner.prepare(new GraphLabRunnable(), resources)
                .withArguments(runnableName, arguments.toArray())
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)));

        if (debug) {
            preparer.enableDebugging(true);
        }

        final TwillController controller = preparer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.debug("shutting down");
                controller.stopAndWait();
                twillRunner.stopAndWait();
            }
        });

        try {
            /*
            // Try to catch the debug port.
            if (debug) {
                waitForDebugPort(controller, runnableName, 300);
            }
            */

            Services.getCompletionFuture(controller).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        LOG.debug("after shutting down");
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                "java" +
                " -cp graphLab-twill-1.0-SNAPSHOT.jar" +
                " org.lab41.graphLab.twill.Main" +
                " [options]" +
                " zookeeper-address" +
                " graphLab-path" +
                " input-path" +
                " input-format" +
                " output-path",
                options);
    }

    private static boolean waitForDebugPort(TwillController controller, String runnable, int timeLimit) throws InterruptedException {
        long millis = 0;
        while (millis < 1000 * timeLimit) {
            ResourceReport report = controller.getResourceReport();
            if (report == null || report.getRunnableResources(runnable) == null) {
                continue;
            }
            for (TwillRunResources resources : report.getRunnableResources(runnable)) {
                if (resources.getDebugPort() != null) {
                    System.out.println("runnable debug port: " + resources.getHost() + ":" + resources.getDebugPort());
                    return true;
                }
            }
            TimeUnit.MILLISECONDS.sleep(100);
            millis += 100;
        }
        return false;
    }
}
