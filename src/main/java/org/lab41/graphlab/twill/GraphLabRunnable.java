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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.twill.api.*;
import org.apache.twill.synchronization.DoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
* GraphLabRunnable drives a single instance of graphlab.
*/
public class GraphLabRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(GraphLabRunnable.class);

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
        Preconditions.checkNotNull(arguments);
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
        try {
            doBarrier();

            // FIXME: work around a Twill bug where Kafka won't send all the messages unless we sleep for a little bit.
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignored) {
            }
        } catch (Exception e) {
            LOG.error("error", e);
            e.printStackTrace();
        }
    }

    private void doBarrier() throws Exception {
        TwillContext context = getContext();
        Preconditions.checkNotNull(context);

        int instanceCount = context.getInstanceCount();
        String runId = context.getApplicationRunId().getId();

        LOG.debug("creating barrier {} with {} parties", runId, instanceCount);

        DoubleBarrier barrier = this.getContext().getDoubleBarrier(runId, instanceCount);

        LOG.debug("entering barrier");

        try {
            barrier.enter(BARRIER_WAIT_TIME, BARRIER_WAIT_UNIT);

            LOG.debug("entered barrier");

            runProcess(instanceCount);

        } finally {
            LOG.debug("leaving barrier");

            barrier.leave(BARRIER_WAIT_TIME, BARRIER_WAIT_UNIT);

            LOG.debug("left barrier");
        }
    }

    private void runProcess(int instanceCount) throws ExecutionException, InterruptedException, IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());

        File graphLabPath = arguments.getGraphLabPath();
        Preconditions.checkNotNull(graphLabPath, "graphlab path is null");
        Preconditions.checkArgument(graphLabPath.exists(), "graphlab path does not exist");

        Path inputPath = arguments.getInputPath();
        Preconditions.checkNotNull(inputPath, "input path is null");
        Preconditions.checkNotNull(fileSystem.exists(inputPath), "input path does not exist");

        String inputFormat = arguments.getInputFormat();
        Preconditions.checkNotNull(inputFormat, "input format is null");

        Path outputPath = arguments.getOutputPath();
        Preconditions.checkNotNull(outputPath, "output path is null");

        String zkStr = System.getenv("TWILL_ZK_CONNECT");
        Preconditions.checkNotNull(zkStr);

        // Start building up the command line.
        List<String> args = new ArrayList<>();
        args.add(graphLabPath.toString());
        args.add("--graph");
        args.add(inputPath.toString());
        args.add("--format");
        args.add(inputFormat);

        // We need to treat some algorithms specially.
        String commandName = graphLabPath.getName();

        switch (commandName) {
            case "simple_coloring":
                args.add("output");
                args.add(outputPath.toString());
                break;
            case "TSC":
                // do nothing. TSC outputs to stdout, so we'll have to capture it ourselves.
                break;
            default:
                args.add("--saveprefix");
                args.add(outputPath.toString());
                break;
        }

        ProcessBuilder processBuilder = new ProcessBuilder(args);

        Map<String, String> env = processBuilder.environment();

        env.clear();
        env.put("CLASSPATH", getHadoopClassPath());
        env.put("ZK_SERVERS", zkStr);
        env.put("ZK_JOBNAME", "graphLab-workers");
        env.put("ZK_NUMNODES", Integer.toString(instanceCount));

        if (!commandName.equals("TSC")) {
            processBuilder.redirectErrorStream(true);
        }

        Process process = processBuilder.start();

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<Void> stdoutFuture;

            if (commandName.equals("TSC")) {
                // The TSC outputs to stdout, so capture and redirect it to our file.
                stdoutFuture = executor.submit(captureInputStream(fileSystem, process.getInputStream(), outputPath));
            } else {
                // Otherwise, write the output to the log file.
                stdoutFuture = executor.submit(logInputStream(process.getInputStream()));
            }

            // Also write the stderr to the log file.
            Future<Void> stderrFuture = executor.submit(logInputStream(process.getErrorStream()));

            // Ignore errors for now.
            process.waitFor();

            stdoutFuture.get();
            stderrFuture.get();
        } finally {
            executor.shutdown();
        }
    }

    /**
     * GraphLab requires all the hadoop path globs to be expanded.
     * @return the classpath.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private String getHadoopClassPath() throws IOException, InterruptedException, ExecutionException {

        List<String> args = Lists.newArrayList();

        String hadoopCommonHome = System.getenv("HADOOP_COMMON_HOME");

        if (hadoopCommonHome == null) {
            args.add("hadoop");
        } else {
            args.add(hadoopCommonHome + "/bin/hadoop");
        }

        args.add("classpath");

        ProcessBuilder processBuilder = new ProcessBuilder(args);

        Map<String, String> env = processBuilder.environment();

        // Inside a yarn application, HADOOP_CONF_DIR points at a path specific to the node manager and is not
        // intended to be used by other programs.
        env.remove("HADOOP_CONF_DIR");

        String hadoopClientConfDir = env.get("HADOOP_CLIENT_CONF_DIR");
        if (hadoopClientConfDir != null) {
            env.put("HADOOP_CONF_DIR", hadoopClientConfDir);
        }

        Process process = processBuilder.start();

        StringWriter writer = new StringWriter();
        IOUtils.copy(process.getInputStream(), writer, Charsets.US_ASCII);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Void> errFuture = executor.submit(logInputStream(process.getErrorStream()));

            process.waitFor();
            errFuture.get();
        } finally {
            executor.shutdown();
        }

        String classPath = writer.toString();
        LOG.info("hadoop classpath: " + classPath);

        // Sometimes the classpath includes globs.
        List<String> classPathList = Lists.newArrayList();

        for (String pattern : classPath.split(File.pathSeparator)) {
            File file = new File(pattern);
            File dir = file.getParentFile();
            String[] children = dir.list(new WildcardFileFilter(file.getName()));

            if (children != null) {
                for (String path : children) {
                    String f = new File(dir, path).toString();
                    LOG.error(f);
                    classPathList.add(f);
                }
            }
        }

        return Joiner.on(File.pathSeparator).join(classPathList);
    }

    private Callable<Void> captureInputStream(final FileSystem fileSystem, final InputStream is, final Path outputPath) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (FSDataOutputStream os = fileSystem.create(outputPath)) {
                    IOUtils.copy(is, os);
                }
                return null;
            }
        };
    }

    private Callable<Void> logInputStream(final InputStream is) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charsets.US_ASCII))) {
                    String line = reader.readLine();
                    while (line != null) {
                        LOG.info(line);
                        line = reader.readLine();
                    }
                }
                return null;
            }
        };
    }

    public static class Arguments {
        private final File graphLabPath;
        private final Path inputPath;
        private final String inputFormat;
        private final Path outputPath;

        public Arguments(File graphLabPath, Path inputPath, String inputFormat, Path outputPath) {
            this.graphLabPath = graphLabPath;
            this.inputPath = inputPath;
            this.inputFormat = inputFormat;
            this.outputPath = outputPath;
        }

        public File getGraphLabPath() {
            return graphLabPath;
        }

        public Path getInputPath() {
            return inputPath;
        }

        public String getInputFormat() {
            return inputFormat;
        }

        public Path getOutputPath() {
            return outputPath;
        }

        public String[] toArray() {
            String[] result = new String[4];
            result[0] = graphLabPath.toString();
            result[1] = inputPath.toString();
            result[2] = inputFormat;
            result[3] = outputPath.toString();
            return result;
        }

        public static Arguments fromArray(String[] args) {
            Preconditions.checkArgument(args.length == 4, "not enough arguments provided");

            Builder builder = new Builder();
            builder.setGraphLabPath(new File(args[0]));
            builder.setInputPath(new Path(args[1]));
            builder.setInputFormat(args[2]);
            builder.setOutputPath(new Path(args[3]));
            return builder.createArguments();
        }

        public static class Builder {
            private File graphLabPath;
            private Path inputPath;
            private String inputFormat;
            private Path outputPath;

            public Builder() {}

            public Builder setGraphLabPath(File graphLabPath) {
                this.graphLabPath = graphLabPath;
                return this;
            }

            public Builder setInputPath(Path inputPath) {
                this.inputPath = inputPath;
                return this;
            }

            public Builder setInputFormat(String inputFormat) {
                this.inputFormat = inputFormat;
                return this;
            }

            public Builder setOutputPath(Path outputPath) {
                this.outputPath = outputPath;
                return this;
            }

            public Arguments createArguments() {
                return new Arguments(graphLabPath, inputPath, inputFormat, outputPath);
            }
        }
    }
}
