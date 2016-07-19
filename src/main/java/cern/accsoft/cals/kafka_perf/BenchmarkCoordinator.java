package cern.accsoft.cals.kafka_perf;

import cern.accsoft.cals.kafka_perf.reporters.FileReporter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BenchmarkCoordinator {
    private static final Charset ZK_DATA_CHARSET = StandardCharsets.US_ASCII;
    private static final Pattern START_COMMAND_PATTERN = Pattern.compile("start\\((?<msize>[0-9]+),(?<topics>[0-9]+)\\)");
    private static final String STOP_STRING = "stop";
    private static final byte[] STOP_STRING_BYTES = STOP_STRING.getBytes(ZK_DATA_CHARSET);
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkCoordinator.class);

    private final List<BenchmarkingService> services;
    private final FileReporter reporter;
    private boolean testIsRunning = false;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    public BenchmarkCoordinator(List<BenchmarkingService> services, FileReporter reporter) {
        this.services = services;
        this.reporter = reporter;
    }

    public void run() throws Exception {
        final CuratorFramework client =
                CuratorFrameworkFactory.newClient(Config.ZK_CONNECTION_STRING, Config.ZK_RETRY_POLICY);

        client.start();

        // register watch listener!
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(Config.TEST_ZNODE_PATH, STOP_STRING_BYTES);
        } catch(KeeperException.NodeExistsException e) {
            LOGGER.info("Test node already exists, no need to create one");
        }

        String data = getTestNodeValueAndSetWatch(client);

        if(!data.equals(STOP_STRING)) {
            throw new IllegalStateException("Tests already started but benchmarking service is not yet ready!");
        }

        /* from now on we are asynchronous, watcher-driven */
        client.getUnhandledErrorListenable().addListener(this::unhandledError);
        client.getCuratorListenable().addListener(this::eventReceived);

        try {
            terminationLatch.await();
        } catch(InterruptedException e) {
            LOGGER.error("Unexpected InterruptedException, returning", e);
        }
    }

    private void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        if(event.getPath() != null && event.getPath().startsWith(Config.TEST_ZNODE_PATH)) {
            switch (event.getType()) {
                case WATCHED:
                    String data = getTestNodeValueAndSetWatch(client);
                    if(data.equals(STOP_STRING)) {
                        stopTest();
                    } else {
                        final Matcher startMatcher = START_COMMAND_PATTERN.matcher(data);
                        if(startMatcher.matches()) {
                            final int messageSize = Integer.valueOf(startMatcher.group("msize"), 10);
                            final int topicCount = Integer.valueOf(startMatcher.group("topics"), 10);

                            startTest(messageSize, topicCount);
                        } else {
                            LOGGER.error("Unrecognized contents of test node: {}", data);
                        }
                    }
                    break;
                default:
                    /* event wasn't interesting, but we must set the watch */
                    setWatch(client);
                    LOGGER.info("Some uninteresting ZK event received: {}", event.toString());
            }
        }
    }

    private void unhandledError(String message, Throwable e) {
        LOGGER.error("Unhandled Curator/Zookeeper error", e);
        terminationLatch.countDown();
    }

    private void startTest(int messageSize, int topics) {
        if(testIsRunning) throw new IllegalStateException("Test is already running!");

        testIsRunning = true;

        services.forEach(s -> s.startTest(messageSize, topics));
        LOGGER.info("Test started with parameters: message_size={}, topics={}", messageSize, topics);
    }

    private void stopTest() {
        if(!testIsRunning) throw new IllegalStateException("Cannot stop test which is not running!");

        List<Long> results = services.stream()
                .map(BenchmarkingService::getMessageCount) // side effect!
                .collect(Collectors.toList());

        reporter.report(results);

        services.forEach(BenchmarkingService::stop);
        testIsRunning = false;

        LOGGER.info("Test stopped");
    }

    private static String getTestNodeValueAndSetWatch(CuratorFramework client) throws Exception {
        final byte[] dataBytes = client.getData().watched().forPath(Config.TEST_ZNODE_PATH);
        return new String(dataBytes, ZK_DATA_CHARSET);
    }

    private static void setWatch(CuratorFramework client) throws Exception {
        client.checkExists().watched().forPath(Config.TEST_ZNODE_PATH);
    }
}
